use chrono::{Duration, Utc};
use cron::Schedule;
use futures_util::future::BoxFuture;
use log::*;
use parking_lot::Mutex;
use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use trailbase_sqlite::params;

use crate::app_state::AppState;
use crate::constants::{DEFAULT_REFRESH_TOKEN_TTL, LOGS_RETENTION_DEFAULT, SESSION_TABLE};

#[allow(unused)]
struct Task {
  name: String,
  schedule: cron::Schedule,
  callback: Arc<dyn Fn() -> BoxFuture<'static, ()> + Sync + Send>,
  handle: tokio::task::AbortHandle,
}

pub struct TaskRegistry {
  tasks: Mutex<Vec<Task>>,
}

impl TaskRegistry {
  pub fn new() -> Self {
    return TaskRegistry {
      tasks: Mutex::new(Vec::new()),
    };
  }

  pub fn add_task<F, Fut>(&self, name: impl Into<String>, schedule: cron::Schedule, f: F)
  where
    F: 'static + Sync + Send + Fn() -> Fut,
    Fut: Sync + Send + Future,
  {
    let fun = Arc::new(f);
    let callback: Arc<dyn Fn() -> BoxFuture<'static, ()> + Sync + Send> = Arc::new(move || {
      let fun = fun.clone();
      return Box::pin(async move {
        fun().await;
      });
    });

    let name: String = name.into();
    let name_clone = name.clone();
    let callback_clone = callback.clone();
    let schedule_clone = schedule.clone();
    let handle = tokio::spawn(async move {
      loop {
        let Some(next) = schedule_clone.upcoming(Utc).next() else {
          break;
        };
        let Ok(duration) = (next - Utc::now()).to_std() else {
          log::warn!("Invalid duration for '{name_clone}': {next:?}");
          continue;
        };

        tokio::time::sleep(duration).await;
        (*callback_clone)().await;
      }

      log::info!("Exited task: '{name_clone}'");
    })
    .abort_handle();

    self.tasks.lock().push(Task {
      name,
      schedule,
      callback,
      handle,
    });
  }
}

impl Drop for TaskRegistry {
  fn drop(&mut self) {
    let tasks = self.tasks.lock();
    for t in tasks.iter() {
      t.handle.abort();
    }
  }
}

#[derive(Default)]
pub struct AbortOnDrop {
  handles: Vec<tokio::task::AbortHandle>,
}

impl AbortOnDrop {
  fn add_periodic_task<F, Fut>(
    &mut self,
    period: Duration,
    f: F,
  ) -> Result<(), chrono::OutOfRangeError>
  where
    F: 'static + Sync + Send + Fn() -> Fut,
    Fut: Sync + Send + Future,
  {
    let p = period.to_std()?;

    let handle = tokio::spawn(async move {
      let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + p, p);
      loop {
        interval.tick().await;
        f().await;
      }
    });

    self.handles.push(handle.abort_handle());

    return Ok(());
  }
}

impl Drop for AbortOnDrop {
  fn drop(&mut self) {
    for h in &self.handles {
      h.abort();
    }
  }
}

pub(super) fn start_periodic_tasks(app_state: &AppState) -> AbortOnDrop {
  let mut tasks = AbortOnDrop::default();

  //               sec  min   hour   day of month   month   day of week   year
  let expression = "0    *     *         *            *         *          *";
  app_state.tasks().add_task(
    "Alive",
    Schedule::from_str(expression).expect("startup"),
    || async {
      info!("alive");
    },
  );

  // Backup job.
  let conn = app_state.conn().clone();
  let backup_file = app_state.data_dir().backup_path().join("backup.db");
  if let Some(backup_interval) = app_state
    .access_config(|c| c.server.backup_interval_sec)
    .map(Duration::seconds)
  {
    tasks
      .add_periodic_task(backup_interval, move || {
        let conn = conn.clone();
        let backup_file = backup_file.clone();

        return async move {
          let result = conn
            .call(|conn| {
              return Ok(conn.backup(
                rusqlite::DatabaseName::Main,
                backup_file,
                /* progress= */ None,
              )?);
            })
            .await;

          match result {
            Ok(_) => info!("Backup complete"),
            Err(err) => error!("Backup failed: {err}"),
          };
        };
      })
      .expect("startup");
  }

  // Logs cleaner.
  let logs_conn = app_state.logs_conn().clone();
  let retention = app_state
    .access_config(|c| c.server.logs_retention_sec)
    .map_or(LOGS_RETENTION_DEFAULT, Duration::seconds);

  if !retention.is_zero() {
    tasks
      .add_periodic_task(retention, move || {
        let logs_conn = logs_conn.clone();

        tokio::spawn(async move {
          let timestamp = (Utc::now() - retention).timestamp();
          match logs_conn
            .execute("DELETE FROM _logs WHERE created < $1", params!(timestamp))
            .await
          {
            Ok(_) => info!("Successfully pruned logs"),
            Err(err) => warn!("Failed to clean up old logs: {err}"),
          };
        })
      })
      .expect("startup");
  }

  // Refresh token cleaner.
  let state = app_state.clone();
  tasks
    .add_periodic_task(Duration::hours(12), move || {
      let state = state.clone();

      tokio::spawn(async move {
        let refresh_token_ttl = state
          .access_config(|c| c.auth.refresh_token_ttl_sec)
          .map_or(DEFAULT_REFRESH_TOKEN_TTL, Duration::seconds);

        let timestamp = (Utc::now() - refresh_token_ttl).timestamp();

        match state
          .user_conn()
          .execute(
            &format!("DELETE FROM '{SESSION_TABLE}' WHERE updated < $1"),
            params!(timestamp),
          )
          .await
        {
          Ok(count) => info!("Successfully pruned {count} old sessions."),
          Err(err) => warn!("Failed to clean up sessions: {err}"),
        };
      })
    })
    .expect("startup");

  // Optimizer
  let conn = app_state.conn().clone();
  app_state.tasks().add_task(
    "Optimizer",
    Schedule::from_str("@daily").expect("startup"),
    move || {
      let conn = conn.clone();

      return async move {
        match conn.execute("PRAGMA optimize", ()).await {
          Ok(_) => info!("Successfully ran query optimizer"),
          Err(err) => warn!("query optimizer failed: {err}"),
        };
      };
    },
  );

  return tasks;
}

#[cfg(test)]
mod tests {
  use super::*;

  use cron::Schedule;
  use std::str::FromStr;

  #[tokio::test]
  async fn test_scheduler() {
    let registry = TaskRegistry::new();

    let (sender, receiver) = async_channel::unbounded::<()>();

    //               sec  min   hour   day of month   month   day of week   year
    let expression = "*    *     *         *            *         *          *";
    registry.add_task("Foo", Schedule::from_str(expression).unwrap(), move || {
      let sender = sender.clone();
      return async move {
        sender.send(()).await.unwrap();
      };
    });

    receiver.recv().await.unwrap();
  }
}
