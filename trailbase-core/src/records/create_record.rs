use axum::extract::{Json, Path, Query, State};
use axum::response::{IntoResponse, Redirect, Response};
use base64::prelude::*;
use serde::{Deserialize, Serialize};
use trailbase_sqlite::schema::FileUploadInput;
use utoipa::{IntoParams, ToSchema};

use crate::app_state::AppState;
use crate::auth::user::User;
use crate::extract::Either;
use crate::records::json_to_sql::{InsertQueryBuilder, JsonRow, LazyParams, Params};
use crate::records::{Permission, RecordError};
use crate::schema::ColumnDataType;

#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
pub struct CreateRecordQuery {
  pub redirect_to: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, ToSchema)]
pub struct CreateRecordResponse {
  /// Safe-url base64 encoded id of the newly created record.
  pub ids: Vec<String>,
}

fn extract_record(value: serde_json::Value) -> Result<JsonRow, RecordError> {
  return Ok(match value {
    serde_json::Value::Object(record) => record,
    _ => {
      return Err(RecordError::BadRequest("Expected single record"));
    }
  });
}

fn extract_records(value: serde_json::Value) -> Result<Vec<JsonRow>, RecordError> {
  return Ok(match value {
    serde_json::Value::Object(record) => vec![record],
    serde_json::Value::Array(records) => {
      let mut record_rows = Vec::<JsonRow>::with_capacity(records.len());
      for record in records {
        let serde_json::Value::Object(record) = record else {
          return Err(RecordError::BadRequest(
            "Expected record or array of records",
          ));
        };
        record_rows.push(record);
      }
      record_rows
    }
    _ => {
      return Err(RecordError::BadRequest(
        "Expected record or array of records",
      ));
    }
  });
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
  let table_metadata = api
    .table_metadata()
    .ok_or_else(|| RecordError::ApiRequiresTable)?;

  let record_and_files: Vec<(JsonRow, Option<Vec<FileUploadInput>>)> = match either_request {
    Either::Json(value) => extract_records(value)?
      .into_iter()
      .map(|r| (r, None))
      .collect(),
    Either::Multipart(value, files) => vec![(extract_record(value)?, Some(files))],
    Either::Form(value) => vec![(extract_record(value)?, None)],
  };

  let mut params_list: Vec<Params> = Vec::with_capacity(record_and_files.len());
  for (record, files) in record_and_files {
    let mut lazy_params = LazyParams::new(table_metadata, record, files);

    api
      .check_record_level_access(
        Permission::Create,
        None,
        Some(&mut lazy_params),
        user.as_ref(),
      )
      .await?;

    let Ok(mut params) = lazy_params.consume() else {
      return Err(RecordError::BadRequest("Parameter conversion"));
    };

    if api.insert_autofill_missing_user_id_columns() {
      let column_names = params.column_names();
      let missing_columns = table_metadata
        .user_id_columns
        .iter()
        .filter_map(|index| {
          let col = &table_metadata.schema.columns[*index];
          if column_names.iter().any(|c| c == &col.name) {
            return None;
          }
          return Some(col.name.clone());
        })
        .collect::<Vec<_>>();

      if !missing_columns.is_empty() {
        if let Some(ref user) = user {
          for col in missing_columns {
            params.push_param(col, trailbase_sqlite::Value::Blob(user.uuid.into()));
          }
        }
      }
    }

    params_list.push(params);
  }

  let pk_column = api.record_pk_column();
  let record_ids = match params_list.len() {
    0 => {
      return Err(RecordError::Internal("".into()));
    }
    1 => {
      let row = InsertQueryBuilder::run(
        &state,
        params_list.swap_remove(0),
        api.insert_conflict_resolution_strategy(),
        Some(&pk_column.name),
      )
      .await
      .map_err(|err| RecordError::Internal(err.into()))?;

      // TODO: move conversion into IsertQueryBuilder::run.
      vec![match pk_column.data_type {
        ColumnDataType::Blob => BASE64_URL_SAFE.encode(
          row
            .get::<[u8; 16]>(0)
            .map_err(|err| RecordError::Internal(err.into()))?,
        ),
        ColumnDataType::Integer => row
          .get::<i64>(0)
          .map_err(|err| RecordError::Internal(err.into()))?
          .to_string(),
        _ => {
          return Err(RecordError::Internal(
            format!("Unexpected data type: {:?}", pk_column.data_type).into(),
          ));
        }
      }]
    }
    _ => InsertQueryBuilder::run_bulk(
      &state,
      params_list,
      api.insert_conflict_resolution_strategy(),
      Some(&pk_column.name),
      pk_column.data_type,
    )
    .await
    .map_err(|err| RecordError::Internal(err.into()))?,
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
  use crate::config::proto::PermissionFlag;
  use crate::records::test_utils::*;
  use crate::records::*;
  use crate::util::id_to_b64;

  #[tokio::test]
  async fn test_record_api_create() -> Result<(), anyhow::Error> {
    let state = test_state(None).await?;
    let conn = state.conn();

    create_chat_message_app_tables(&state).await?;
    let room = add_room(conn, "room0").await?;
    let password = "Secret!1!!";

    // Register message table as api with moderator read access.
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
          "EXISTS(SELECT 1 FROM room_members AS m WHERE _USER_.id = _REQ_._owner AND m.user = _USER_.id AND m.room = _REQ_.room )".to_string(),
        ),
        ..Default::default()
      },
    )
    .await?;

    let user_x_email = "user_x@bar.com";
    let user_x = create_user_for_test(&state, user_x_email, password)
      .await?
      .into_bytes();
    let user_x_token = login_with_password(&state, user_x_email, password).await?;

    add_user_to_room(conn, user_x, room).await?;

    let user_y_email = "user_y@test.com";
    let user_y = create_user_for_test(&state, user_y_email, password)
      .await?
      .into_bytes();

    let user_y_token = login_with_password(&state, user_y_email, password).await?;

    {
      // User X can post to the room, they're a member of
      let json = serde_json::json!({
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

    {
      // User X can post as a different "_owner".
      let json = serde_json::json!({
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
      // User Y is not a member and cannot post to the room.
      let json = serde_json::json!({
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

    return Ok(());
  }

  #[tokio::test]
  async fn test_record_api_create_integer_id() -> Result<(), anyhow::Error> {
    let state = test_state(None).await?;
    let conn = state.conn();

    create_chat_message_app_tables_integer(&state).await?;
    let room = add_room(conn, "room0").await?;
    let password = "Secret!1!!";

    // Register message table as api with moderator read access.
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
          "EXISTS(SELECT 1 FROM room_members AS m WHERE _USER_.id = _REQ_._owner AND m.user = _USER_.id AND m.room = _REQ_.room )".to_string(),
        ),
        ..Default::default()
      },
    )
    .await?;

    let user_x_email = "user_x@bar.com";
    let user_x = create_user_for_test(&state, user_x_email, password)
      .await?
      .into_bytes();
    let user_x_token = login_with_password(&state, user_x_email, password).await?;

    add_user_to_room(conn, user_x, room).await?;

    {
      // User X can post to the room, they're a member of
      let json = serde_json::json!({
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

    return Ok(());
  }
}
