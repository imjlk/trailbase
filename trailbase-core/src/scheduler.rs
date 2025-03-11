use chrono::{DateTime, Duration, Utc};
use cron::Schedule;
use futures_util::future::BoxFuture;
use log::*;
use parking_lot::Mutex;
use std::future::Future;
use std::str::FromStr;
use std::sync::{
  atomic::{AtomicI64, Ordering},
  Arc,
};
use trailbase_sqlite::params;

use crate::app_state::AppState;
use crate::constants::{DEFAULT_REFRESH_TOKEN_TTL, LOGS_RETENTION_DEFAULT, SESSION_TABLE};

static TASK_COUNTER: AtomicI64 = AtomicI64::new(0);

type CallbackError = Box<dyn std::error::Error + Sync + Send>;
type CallbackFunction = dyn Fn() -> BoxFuture<'static, Result<(), CallbackError>> + Sync + Send;
type LatestCallbackExecution = Option<(DateTime<Utc>, Option<CallbackError>)>;

pub trait CallbackResultTrait {
  fn into_result(self) -> Result<(), CallbackError>;
}

impl CallbackResultTrait for () {
  fn into_result(self) -> Result<(), CallbackError> {
    return Ok(());
  }
}

impl<T: Into<CallbackError>> CallbackResultTrait for Result<(), T> {
  fn into_result(self) -> Result<(), CallbackError> {
    return self.map_err(|e| e.into());
  }
}

#[allow(unused)]
struct Task {
  id: i64,
  name: String,
  schedule: Schedule,
  callback: Arc<CallbackFunction>,

  handle: Option<tokio::task::AbortHandle>,
  latest: Arc<Mutex<LatestCallbackExecution>>,
}

pub struct TaskRegistry {
  tasks: Mutex<Vec<Task>>,
}

impl Task {
  fn new(name: String, schedule: Schedule, callback: Arc<CallbackFunction>) -> Self {
    return Task {
      id: TASK_COUNTER.fetch_add(1, Ordering::SeqCst),
      name,
      schedule,
      callback,
      handle: None,
      latest: Arc::new(Mutex::new(None)),
    };
  }

  fn start(&mut self) {
    let name = self.name.clone();
    let callback = self.callback.clone();
    let schedule = self.schedule.clone();
    let latest = self.latest.clone();

    let handle = tokio::spawn(async move {
      loop {
        let now = Utc::now();
        let Some(next) = schedule.upcoming(Utc).next() else {
          break;
        };
        let Ok(duration) = (next - now).to_std() else {
          log::warn!("Invalid duration for '{name}': {next:?}");
          continue;
        };

        tokio::time::sleep(duration).await;

        let result = (*callback)().await;
        *latest.lock() = Some((Utc::now(), result.err()));
      }

      log::info!("Exited task: '{name}'");
    });

    self.handle = Some(handle.abort_handle());
  }

  fn stop(&mut self) {
    if let Some(ref handle) = self.handle {
      handle.abort();
    }
    self.handle = None;
  }

  fn running(&self) -> bool {
    if let Some(ref handle) = self.handle {
      return !handle.is_finished();
    }
    return false;
  }
}

impl TaskRegistry {
  pub fn new() -> Self {
    return TaskRegistry {
      tasks: Mutex::new(Vec::new()),
    };
  }

  pub fn add_task<O, F, Fut>(&self, name: impl Into<String>, schedule: Schedule, f: F)
  where
    F: 'static + Sync + Send + Fn() -> Fut,
    Fut: Sync + Send + Future<Output = O>,
    O: CallbackResultTrait,
  {
    let fun = Arc::new(f);
    let callback: Arc<CallbackFunction> = Arc::new(move || {
      let fun = fun.clone();
      return Box::pin(async move {
        return fun().await.into_result();
      });
    });

    self.tasks.lock().push({
      let mut task = Task::new(name.into(), schedule, callback);
      task.start();
      task
    });
  }
}

impl Drop for TaskRegistry {
  fn drop(&mut self) {
    let mut tasks = self.tasks.lock();
    for t in tasks.iter_mut() {
      t.stop();
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

  //               sec  min   hour   day of month   month   day of week  year
  let expression = "0    *     *         *            *         *         *";
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
        conn.execute("PRAGMA optimize", ()).await.map_err(|err| {
          warn!("query optimizer failed: {err}");
          return err;
        })?;

        Ok::<(), trailbase_sqlite::Error>(())
      };
    },
  );

  return tasks;
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_cron() {
    //               sec      min   hour   day of month   month   day of week  year
    let expression = "*/100   *     *         *            *         *          *";
    assert!(Schedule::from_str(expression).is_err());

    let expression = "*/40    *     *         *            *         *          *";
    Schedule::from_str(expression).unwrap();
  }

  #[tokio::test]
  async fn test_scheduler() {
    // NOTE: Cron is time and not interval based, i.e. something like every 100s is not
    // representable in a single cron spec. Make sure that our cron parser detects that proerly,
    // e.g. croner does not and will just produce buggy intervals.
    // NOTE: Interval-based scheduling is generally problematic because it drifts. For something
    // like a backup you certainly want to control when and not how often it happens (e.g. at
    // night).
    let registry = TaskRegistry::new();

    let (sender, receiver) = async_channel::unbounded::<()>();

    //               sec  min   hour   day of month   month   day of week  year
    let expression = "*    *     *         *            *         *         *";
    registry.add_task(
      "Test Task",
      Schedule::from_str(expression).unwrap(),
      move || {
        let sender = sender.clone();
        return async move {
          sender.send(()).await.unwrap();
          Err("result")
        };
      },
    );

    receiver.recv().await.unwrap();

    let tasks = registry.tasks.lock();
    let latest = tasks[0].latest.lock();
    let (_timestamp, err) = latest.as_ref().unwrap();
    assert_eq!(err.as_ref().unwrap().to_string(), "result");
  }
}
