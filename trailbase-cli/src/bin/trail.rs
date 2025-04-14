#![allow(clippy::needless_return)]

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use chrono::TimeZone;
use clap::{CommandFactory, Parser};
use log::*;
use serde::Deserialize;
use std::rc::Rc;
use tokio::{fs, io::AsyncWriteExt};
use trailbase::{
  api::{self, init_app_state, Email, InitArgs, JsonSchemaMode, TokenClaims},
  constants::USER_TABLE,
  DataDir, Server, ServerOptions,
};

use trailbase_cli::{AdminSubCommands, DefaultCommandLineArgs, SubCommands, UserSubCommands};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn init_logger(dev: bool) {
  // SWC is very spammy in in debug builds and complaints about source maps when compiling
  // typescript to javascript. Since we don't care about source maps and didn't find a better
  // option to mute the errors, turn it off in debug builds.
  const DEFAULT: &str =
    "info,refinery_core=warn,trailbase_refinery_core=warn,tracing::span=warn,swc_ecma_codegen=off";

  env_logger::Builder::from_env(if dev {
    env_logger::Env::new().default_filter_or(format!("{DEFAULT},trailbase=debug"))
  } else {
    env_logger::Env::new().default_filter_or(DEFAULT)
  })
  .format_timestamp_micros()
  .init();
}

#[derive(Deserialize)]
struct DbUser {
  id: [u8; 16],
  email: String,
  verified: bool,
  created: i64,
  updated: i64,
}

impl DbUser {
  fn uuid(&self) -> uuid::Uuid {
    uuid::Uuid::from_bytes(self.id)
  }
}

async fn get_user_by_email(
  conn: &trailbase_sqlite::Connection,
  email: &str,
) -> Result<DbUser, BoxError> {
  if let Some(user) = conn
    .read_query_value::<DbUser>(
      format!("SELECT * FROM {USER_TABLE} WHERE email = $1"),
      (email.to_string(),),
    )
    .await?
  {
    return Ok(user);
  }
  return Err("not found".into());
}

async fn async_main() -> Result<(), BoxError> {
  let args = DefaultCommandLineArgs::parse();
  let data_dir = DataDir(args.data_dir.clone());

  match args.cmd {
    Some(SubCommands::Run(cmd)) => {
      init_logger(cmd.dev);

      let app = Server::init(ServerOptions {
        data_dir,
        address: cmd.address,
        admin_address: cmd.admin_address,
        public_dir: cmd.public_dir.map(|p| p.into()),
        log_responses: cmd.dev || cmd.stderr_logging,
        dev: cmd.dev,
        demo: cmd.demo,
        disable_auth_ui: cmd.disable_auth_ui,
        cors_allowed_origins: cmd.cors_allowed_origins,
        js_runtime_threads: cmd.js_runtime_threads,
        tls_key: None,
        tls_cert: None,
      })
      .await?;

      app.serve().await?;
    }
    #[cfg(feature = "openapi")]
    Some(SubCommands::OpenApi { cmd }) => {
      init_logger(false);

      use trailbase_cli::OpenApiSubCommands;
      use utoipa::OpenApi;
      use utoipa_swagger_ui::SwaggerUi;

      let run_server = |port: u16| async move {
        let router = axum::Router::new().merge(
          SwaggerUi::new("/docs").url("/api/openapi.json", trailbase::openapi::Doc::openapi()),
        );

        let addr = format!("localhost:{port}");
        let listener = tokio::net::TcpListener::bind(addr.clone()).await.unwrap();
        log::info!("docs @ http://{addr}/docs 🚀");

        axum::serve(listener, router).await.unwrap();
      };

      match cmd {
        Some(OpenApiSubCommands::Print) => {
          let json = trailbase::openapi::Doc::openapi().to_pretty_json()?;
          println!("{json}");
        }
        Some(OpenApiSubCommands::Run { port }) => {
          run_server(port).await;
        }
        None => {
          run_server(4004).await;
        }
      }
    }
    Some(SubCommands::Schema(cmd)) => {
      init_logger(false);

      let (_new_db, state) =
        init_app_state(DataDir(args.data_dir), None, InitArgs::default()).await?;

      let api_name = &cmd.api;
      let Some(api) = state.lookup_record_api(api_name) else {
        return Err(format!("Could not find api: '{api_name}'").into());
      };

      let mode: Option<JsonSchemaMode> = cmd.mode.map(|m| m.into());
      let json_schema = trailbase::api::build_api_json_schema(&state, &api, mode)?;

      println!("{}", serde_json::to_string_pretty(&json_schema)?);
    }
    Some(SubCommands::Migration { suffix }) => {
      init_logger(false);

      let filename = api::new_unique_migration_filename(suffix.as_deref().unwrap_or("update"));
      let path = data_dir.migrations_path().join(filename);

      let mut migration_file = fs::File::create_new(&path).await?;
      migration_file
        .write_all(b"-- new database migration\n")
        .await?;

      println!("Created empty migration file: {path:?}");
    }
    Some(SubCommands::Admin { cmd }) => {
      init_logger(false);

      let conn = trailbase_sqlite::Connection::new(
        || api::connect_sqlite(Some(data_dir.main_db_path()), None),
        None,
      )?;

      match cmd {
        Some(AdminSubCommands::List) => {
          let users = conn
            .read_query_values::<DbUser>(format!("SELECT * FROM {USER_TABLE} WHERE admin > 0"), ())
            .await?;

          println!("{: >36}\temail\tcreated\tupdated", "id");
          for user in users {
            let id = user.uuid();

            println!(
              "{id}\t{}\t{created:?}\t{updated:?}",
              user.email,
              created = chrono::Utc.timestamp_opt(user.created, 0),
              updated = chrono::Utc.timestamp_opt(user.updated, 0),
            );
          }
        }
        Some(AdminSubCommands::Demote { email }) => {
          conn
            .execute(
              format!("UPDATE {USER_TABLE} SET admin = FALSE WHERE email = $1"),
              (email.clone(),),
            )
            .await?;

          println!("'{email}' has been demoted");
        }
        Some(AdminSubCommands::Promote { email }) => {
          conn
            .execute(
              format!("UPDATE {USER_TABLE} SET admin = TRUE WHERE email = $1"),
              (email.clone(),),
            )
            .await?;

          println!("'{email}' is now an admin");
        }
        None => {
          DefaultCommandLineArgs::command()
            .find_subcommand_mut("admin")
            .map(|cmd| cmd.print_help());
        }
      };
    }
    Some(SubCommands::User { cmd }) => {
      init_logger(false);

      let data_dir = DataDir(args.data_dir);
      let conn = trailbase_sqlite::Connection::new(
        || api::connect_sqlite(Some(data_dir.main_db_path()), None),
        None,
      )?;

      match cmd {
        Some(UserSubCommands::ResetPassword { email, password }) => {
          if get_user_by_email(&conn, &email).await.is_err() {
            return Err(format!("User with email='{email}' not found.").into());
          }
          api::force_password_reset(&conn, email.clone(), password).await?;

          println!("Password updated for '{email}'");
        }
        Some(UserSubCommands::MintToken { email }) => {
          let user = get_user_by_email(&conn, &email).await?;
          let jwt = api::JwtHelper::init_from_path(&data_dir).await?;

          if !user.verified {
            warn!("User '{email}' not verified");
          }

          let claims = TokenClaims::new(
            user.verified,
            user.uuid(),
            user.email,
            chrono::Duration::hours(12),
          );
          let token = jwt.encode(&claims)?;

          println!("Bearer {token}");
        }
        None => {
          DefaultCommandLineArgs::command()
            .find_subcommand_mut("user")
            .map(|cmd| cmd.print_help());
        }
      };
    }
    Some(SubCommands::Email(cmd)) => {
      init_logger(false);

      let (_new_db, state) =
        init_app_state(DataDir(args.data_dir), None, InitArgs::default()).await?;

      let email = Email::new(&state, &cmd.to, cmd.subject, cmd.body)?;
      email.send().await?;

      let c = state.get_config().email;
      match (c.smtp_host, c.smtp_port, c.smtp_username, c.smtp_password) {
        (Some(host), Some(port), Some(username), Some(_)) => {
          println!("Sent email using: {username}@{host}:{port}");
        }
        _ => {
          println!("Sent email using system's sendmail");
        }
      };
    }
    None => {
      let _ = DefaultCommandLineArgs::command().print_help();
    }
  }

  Ok(())
}

fn main() -> Result<(), BoxError> {
  let runtime = Rc::new(
    tokio::runtime::Builder::new_multi_thread()
      .enable_all()
      .build()?,
  );
  return runtime.block_on(async_main());
}
