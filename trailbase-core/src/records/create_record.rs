use axum::extract::{Json, Path, Query, State};
use axum::response::{IntoResponse, Redirect, Response};
use base64::prelude::*;
use serde::{Deserialize, Serialize};
use trailbase_schema::FileUploadInput;
use utoipa::{IntoParams, ToSchema};

use crate::app_state::AppState;
use crate::auth::user::User;
use crate::extract::Either;
use crate::records::params::{JsonRow, LazyParams, Params};
use crate::records::query_builder::InsertQueryBuilder;
use crate::records::{Permission, RecordError};
use crate::util::uuid_to_b64;

#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
pub struct CreateRecordQuery {
  /// Redirect user to this address upon successful record creation.
  /// This only exists to support insertions via static HTML form actions.
  ///
  /// We may want to have a different on-error redirect to better support the static HTML use-case.
  pub redirect_to: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct CreateRecordResponse {
  /// Safe-url base64 encoded id of the newly created record.
  pub ids: Vec<String>,
}

#[inline]
fn extract_record(value: serde_json::Value) -> Result<JsonRow, RecordError> {
  return Ok(match value {
    serde_json::Value::Object(record) => record,
    _ => {
      return Err(RecordError::BadRequest("Expected single record"));
    }
  });
}

type RecordAndFiles = (JsonRow, Option<Vec<FileUploadInput>>);

#[inline]
fn extract_records(value: serde_json::Value) -> Result<Vec<RecordAndFiles>, RecordError> {
  return match value {
    serde_json::Value::Object(record) => Ok(vec![(record, None)]),
    serde_json::Value::Array(records) => {
      if records.len() > 1024 {
        return Err(RecordError::BadRequest("Bulk creation exceeds limit: 1024"));
      }

      records
        .into_iter()
        .map(|record| {
          let serde_json::Value::Object(record) = record else {
            return Err(RecordError::BadRequest(
              "Expected record or array of records",
            ));
          };

          return Ok((record, None));
        })
        .collect()
    }
    _ => Err(RecordError::BadRequest(
      "Expected record or array of records",
    )),
  };
}

#[inline]
fn extract_record_id(value: rusqlite::types::Value) -> Result<String, trailbase_sqlite::Error> {
  return match value {
    rusqlite::types::Value::Blob(blob) => Ok(BASE64_URL_SAFE.encode(blob)),
    rusqlite::types::Value::Text(text) => Ok(text),
    rusqlite::types::Value::Integer(i) => Ok(i.to_string()),
    _ => Err(trailbase_sqlite::Error::Other(
      "Unexpected data type".into(),
    )),
  };
}

/// Create new record.
#[utoipa::path(
  post,
  path = "/:name",
  params(CreateRecordQuery),
  request_body = serde_json::Value,
  responses(
    (status = 200, description = "Record id of successful insertion.", body = CreateRecordResponse),
  )
)]
pub async fn create_record_handler(
  State(state): State<AppState>,
  Path(api_name): Path<String>,
  Query(create_record_query): Query<CreateRecordQuery>,
  user: Option<User>,
  either_request: Either<serde_json::Value>,
) -> Result<Response, RecordError> {
  let Some(api) = state.lookup_record_api(&api_name) else {
    return Err(RecordError::ApiNotFound);
  };
  if !api.is_table() {
    return Err(RecordError::ApiRequiresTable);
  }

  let records_and_files: Vec<RecordAndFiles> = match either_request {
    Either::Json(value) => extract_records(value)?,
    Either::Multipart(value, files) => vec![(extract_record(value)?, Some(files))],
    Either::Form(value) => vec![(extract_record(value)?, None)],
  };

  let mut params_list: Vec<Params> = Vec::with_capacity(records_and_files.len());
  for (mut record, files) in records_and_files {
    if api.insert_autofill_missing_user_id_columns() {
      if let Some(ref user) = user {
        for column_index in api.user_id_columns() {
          let col_name = &api.columns()[*column_index].name;
          if !record.contains_key(col_name) {
            record.insert(
              col_name.to_owned(),
              serde_json::Value::String(uuid_to_b64(&user.uuid)),
            );
          }
        }
      }
    }

    let mut lazy_params = LazyParams::new(&api, record, files);

    // NOTE: We're currently serializing the async checks, we could parallelize them however it's
    // unclear if this would be much faster.
    api
      .check_record_level_access(
        Permission::Create,
        None,
        Some(&mut lazy_params),
        user.as_ref(),
      )
      .await?;

    params_list.push(
      lazy_params
        .consume()
        .map_err(|_| RecordError::BadRequest("Parameter conversion"))?,
    );
  }

  let (_index, pk_column) = api.record_pk_column();
  let record_ids: Vec<String> = match params_list.len() {
    0 => {
      return Err(RecordError::BadRequest("no values provided"));
    }
    1 => {
      let record_id = InsertQueryBuilder::run(
        &state,
        api.table_name(),
        api.insert_conflict_resolution_strategy(),
        &pk_column.name,
        api.has_file_columns(),
        params_list.swap_remove(0),
      )
      .await
      .map_err(|err| RecordError::Internal(err.into()))?;

      vec![extract_record_id(record_id)?]
    }
    _ => {
      let record_ids = InsertQueryBuilder::run_bulk(
        &state,
        api.table_name(),
        api.insert_conflict_resolution_strategy(),
        &pk_column.name,
        api.has_file_columns(),
        params_list,
      )
      .await
      .map_err(|err| RecordError::Internal(err.into()))?;

      record_ids
        .into_iter()
        .map(extract_record_id)
        .collect::<Result<Vec<_>, _>>()?
    }
  };

  if let Some(redirect_to) = create_record_query.redirect_to {
    return Ok(Redirect::to(&redirect_to).into_response());
  }

  return Ok(Json(CreateRecordResponse { ids: record_ids }).into_response());
}

#[cfg(test)]
mod test {
  use super::*;
  use crate::admin::user::*;
  use crate::app_state::*;
  use crate::auth::api::login::login_with_password;
  use crate::config::proto::{ConflictResolutionStrategy, PermissionFlag, RecordApiConfig};
  use crate::records::test_utils::*;
  use crate::records::*;
  use crate::test::unpack_json_response;
  use crate::util::{id_to_b64, uuid_to_b64};

  use serde_json::json;

  #[tokio::test]
  async fn test_simple_record_api_create() {
    let state = test_state(None).await.unwrap();

    state
      .conn()
      .execute(
        r#"
      CREATE TABLE simple (
        owner   BLOB PRIMARY KEY CHECK(is_uuid_v7(owner)) REFERENCES _user,
        value   INTEGER
      ) STRICT;
      "#,
        (),
      )
      .await
      .unwrap();

    state.table_metadata().invalidate_all().await.unwrap();

    add_record_api_config(
      &state,
      RecordApiConfig {
        name: Some("simple_api".to_string()),
        table_name: Some("simple".to_string()),
        conflict_resolution: Some(ConflictResolutionStrategy::Replace as i32),
        acl_authenticated: [PermissionFlag::Create as i32, PermissionFlag::Read as i32].into(),
        create_access_rule: Some("_USER_.id = _REQ_.owner".to_string()),
        ..Default::default()
      },
    )
    .await
    .unwrap();

    let password = "Secret!1!!";
    let user_x_email = "user_x@bar.com";
    let user_x = create_user_for_test(&state, user_x_email, password)
      .await
      .unwrap()
      .into_bytes();
    let user_x_token = login_with_password(&state, user_x_email, password)
      .await
      .unwrap();

    // Test conflict resolution strategy replacement.
    for idx in 5..10 {
      let _ = create_record_handler(
        State(state.clone()),
        Path("simple_api".to_string()),
        Query(CreateRecordQuery::default()),
        User::from_auth_token(&state, &user_x_token.auth_token),
        Either::Json(
          json_row_from_value(json!({
            "owner": id_to_b64(&user_x),
            "value": idx,
          }))
          .unwrap()
          .into(),
        ),
      )
      .await
      .unwrap();
    }

    let value: Option<i64> = state
      .conn()
      .read_query_row_f(
        "SELECT value FROM simple WHERE owner = ?1",
        trailbase_sqlite::params!(user_x),
        |row| row.get(0),
      )
      .await
      .unwrap();
    assert_eq!(value, Some(9));

    {
      // Make sure user.id == owner ACL check works
      let response = create_record_handler(
        State(state.clone()),
        Path("simple_api".to_string()),
        Query(CreateRecordQuery::default()),
        User::from_auth_token(&state, &user_x_token.auth_token),
        Either::Json(
          json_row_from_value(json!({
            "owner": uuid_to_b64(&uuid::Uuid::now_v7()),
            "value": 17,
          }))
          .unwrap()
          .into(),
        ),
      )
      .await;

      assert!(response.is_err());
    }
  }

  #[tokio::test]
  async fn test_record_api_create() {
    let state = test_state(None).await.unwrap();
    let conn = state.conn();

    create_chat_message_app_tables(&state).await.unwrap();
    let room = add_room(conn, "room0").await.unwrap();
    let password = "Secret!1!!";

    // Register message table as api.
    add_record_api(
      &state,
      "messages_api",
      "message",
      Acls {
        authenticated: vec![PermissionFlag::Create, PermissionFlag::Read],
        ..Default::default()
      },
      AccessRules {
        create: Some(
          "_USER_.id = _REQ_._owner AND EXISTS(SELECT 1 FROM room_members AS m WHERE m.user = _USER_.id AND m.room = _REQ_.room)".to_string(),
        ),
        ..Default::default()
      },
    )
    .await.unwrap();

    let user_x_email = "user_x@bar.com";
    let user_x = create_user_for_test(&state, user_x_email, password)
      .await
      .unwrap()
      .into_bytes();
    let user_x_token = login_with_password(&state, user_x_email, password)
      .await
      .unwrap();

    add_user_to_room(conn, user_x, room).await.unwrap();

    let user_y_email = "user_y@test.com";
    let user_y = create_user_for_test(&state, user_y_email, password)
      .await
      .unwrap()
      .into_bytes();

    let user_y_token = login_with_password(&state, user_y_email, password)
      .await
      .unwrap();

    {
      // User X can post to the room, they're a member of
      let json = json!({
        "_owner": id_to_b64(&user_x),
        "room": id_to_b64(&room),
        "data": "user_x message to room",
      });
      let response = create_record_handler(
        State(state.clone()),
        Path("messages_api".to_string()),
        Query(CreateRecordQuery::default()),
        User::from_auth_token(&state, &user_x_token.auth_token),
        Either::Json(json_row_from_value(json).unwrap().into()),
      )
      .await;
      assert!(response.is_ok(), "{response:?}");

      let response: CreateRecordResponse = unpack_json_response(response.unwrap()).await.unwrap();

      assert_eq!(1, response.ids.len());
    }

    {
      // User X can bulk post to the room, they're a member of
      let json = |i: usize| {
        json!({
          "_owner": id_to_b64(&user_x),
          "room": id_to_b64(&room),
          "data": format!("user_x bulk message to room {i}"),
        })
      };

      let response = create_record_handler(
        State(state.clone()),
        Path("messages_api".to_string()),
        Query(CreateRecordQuery::default()),
        User::from_auth_token(&state, &user_x_token.auth_token),
        Either::Json(serde_json::Value::Array(vec![json(0), json(1)])),
      )
      .await;
      assert!(response.is_ok(), "{response:?}");

      let response: CreateRecordResponse = unpack_json_response(response.unwrap()).await.unwrap();

      assert_eq!(2, response.ids.len());
    }

    {
      // User X cannot post as a different "_owner".
      let json = json!({
        "_owner": id_to_b64(&user_y),
        "room": id_to_b64(&room),
        "data": "user_x message to room",
      });
      let response = create_record_handler(
        State(state.clone()),
        Path("messages_api".to_string()),
        Query(CreateRecordQuery::default()),
        User::from_auth_token(&state, &user_x_token.auth_token),
        Either::Json(json_row_from_value(json).unwrap().into()),
      )
      .await;
      assert!(response.is_err(), "{response:?}");
    }

    {
      // Bulk inserts are rolled back in a transaction is second insert fails.
      let count_before: usize = state
        .conn()
        .read_query_row_f("SELECT COUNT(*) FROM message", (), |row| row.get(0))
        .await
        .unwrap()
        .unwrap();

      let json = |user_id: &[u8; 16]| {
        json!({
          "_owner": id_to_b64(user_id),
          "room": id_to_b64(&room),
          "data": "user_x bulk message to room",
        })
      };

      // This should fail because of user_y as _owner.
      let response = create_record_handler(
        State(state.clone()),
        Path("messages_api".to_string()),
        Query(CreateRecordQuery::default()),
        User::from_auth_token(&state, &user_x_token.auth_token),
        Either::Json(serde_json::Value::Array(vec![json(&user_x), json(&user_y)])),
      )
      .await;
      assert!(response.is_err(), "{response:?}");

      let count_after: usize = state
        .conn()
        .read_query_row_f("SELECT COUNT(*) FROM message", (), |row| row.get(0))
        .await
        .unwrap()
        .unwrap();
      assert_eq!(count_before, count_after);
    }

    {
      // User Y is not a member and cannot post to the room.
      let json = json!({
        "room": id_to_b64(&room),
        "data": "user_x message to room",
      });
      let response = create_record_handler(
        State(state.clone()),
        Path("messages_api".to_string()),
        Query(CreateRecordQuery::default()),
        User::from_auth_token(&state, &user_y_token.auth_token),
        Either::Json(json_row_from_value(json).unwrap().into()),
      )
      .await;
      assert!(response.is_err(), "{response:?}");
    }
  }

  #[tokio::test]
  async fn test_record_api_create_integer_id() {
    let state = test_state(None).await.unwrap();
    let conn = state.conn();

    create_chat_message_app_tables_integer(&state)
      .await
      .unwrap();
    let room = add_room(conn, "room0").await.unwrap();
    let password = "Secret!1!!";

    // Register message table as api.
    add_record_api(
      &state,
      "messages_api",
      "message",
      Acls {
        authenticated: vec![PermissionFlag::Create, PermissionFlag::Read],
        ..Default::default()
      },
      AccessRules {
        create: Some(
          "_USER_.id = _REQ_._owner AND EXISTS(SELECT 1 FROM room_members AS m WHERE m.user = _USER_.id AND m.room = _REQ_.room)".to_string(),
        ),
        ..Default::default()
      },
    )
    .await.unwrap();

    let user_x_email = "user_x@bar.com";
    let user_x = create_user_for_test(&state, user_x_email, password)
      .await
      .unwrap()
      .into_bytes();
    let user_x_token = login_with_password(&state, user_x_email, password)
      .await
      .unwrap();

    add_user_to_room(conn, user_x, room).await.unwrap();

    {
      // User X can post to the room, they're a member of
      let json = json!({
        "_owner": id_to_b64(&user_x),
        "room": id_to_b64(&room),
        "data": "user_x message to room",
      });
      let response = create_record_handler(
        State(state.clone()),
        Path("messages_api".to_string()),
        Query(CreateRecordQuery::default()),
        User::from_auth_token(&state, &user_x_token.auth_token),
        Either::Json(json_row_from_value(json).unwrap().into()),
      )
      .await;
      assert!(response.is_ok(), "{response:?}");
    }
  }
}
