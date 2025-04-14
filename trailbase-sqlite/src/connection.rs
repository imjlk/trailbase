use crossbeam_channel::{Receiver, Sender};
// use parking_lot::RwLock;
use rusqlite::fallible_iterator::FallibleIterator;
use rusqlite::hooks::{Action, PreUpdateCase};
use rusqlite::types::Value;
use std::{
  fmt::{self, Debug},
  sync::Arc,
};
use tokio::sync::oneshot;

use crate::error::Error;
pub use crate::params::Params;
use crate::rows::{columns, Column};
pub use crate::rows::{Row, Rows};

#[macro_export]
macro_rules! params {
    () => {
        [] as [$crate::params::ToSqlType]
    };
    ($($param:expr),+ $(,)?) => {
        [$(Into::<$crate::params::ToSqlType>::into($param)),+]
    };
}

#[macro_export]
macro_rules! named_params {
    () => {
        [] as [(&str, $crate::params::ToSqlType)]
    };
    ($($param_name:literal: $param_val:expr),+ $(,)?) => {
        [$(($param_name as &str, Into::<$crate::params::ToSqlType>::into($param_val))),+]
    };
}

/// The result returned on method calls in this crate.
pub type Result<T> = std::result::Result<T, Error>;

type CallFn = Box<dyn FnOnce(&mut rusqlite::Connection) + Send + 'static>;

enum Message {
  Run(CallFn),
  Close(oneshot::Sender<std::result::Result<(), rusqlite::Error>>),
}

#[derive(Clone)]
pub struct Options {
  pub busy_timeout: std::time::Duration,
  pub n_read_threads: usize,
}

impl Default for Options {
  fn default() -> Self {
    return Self {
      busy_timeout: std::time::Duration::from_secs(5),
      n_read_threads: 0,
    };
  }
}

struct ConnectionState {
  reader: Sender<Message>,
  writer: Sender<Message>,
  // lock: Arc<RwLock<()>>,
}

/// A handle to call functions in background thread.
#[derive(Clone)]
pub struct Connection {
  state: Arc<ConnectionState>,
}

impl Connection {
  pub fn new<E>(
    c: impl Fn() -> std::result::Result<rusqlite::Connection, E>,
    opt: Option<Options>,
  ) -> std::result::Result<Self, E> {
    let spawn = |receiver: Receiver<Message>| -> std::result::Result<(), E> {
      let conn = c()?;
      if let Some(timeout) = opt.as_ref().map(|o| o.busy_timeout) {
        conn.busy_timeout(timeout).expect("busy timeout failed");
      }

      std::thread::spawn(move || event_loop(conn, receiver));

      return Ok(());
    };

    let (shared_write_sender, shared_write_receiver) = crossbeam_channel::unbounded::<Message>();
    spawn(shared_write_receiver)?;

    let (shared_read_sender, shared_read_receiver) = crossbeam_channel::unbounded::<Message>();
    let n_read_threads = opt.as_ref().map_or(0, |o| o.n_read_threads);
    for _ in 0..n_read_threads {
      spawn(shared_read_receiver.clone())?;
    }

    return Ok(Self {
      state: Arc::new(ConnectionState {
        reader: if n_read_threads > 0 {
          shared_read_sender
        } else {
          shared_write_sender.clone()
        },
        writer: shared_write_sender,
        // lock: Arc::new(RwLock::new(())),
      }),
    });
  }

  /// Open a new connection to an in-memory SQLite database.
  ///
  /// # Failure
  ///
  /// Will return `Err` if the underlying SQLite open call fails.
  pub fn open_in_memory() -> Result<Self> {
    return Self::new(|| Ok(rusqlite::Connection::open_in_memory()?), None);
  }

  /// Call a function in background thread and get the result
  /// asynchronously.
  ///
  /// # Failure
  ///
  /// Will return `Err` if the database connection has been closed.
  #[inline]
  pub async fn call<F, R>(&self, function: F) -> Result<R>
  where
    F: FnOnce(&mut rusqlite::Connection) -> Result<R> + Send + 'static,
    R: Send + 'static,
  {
    return call_impl(&self.state.writer, function).await;
  }

  #[inline]
  pub async fn call_reader<F, R>(&self, function: F) -> Result<R>
  where
    F: FnOnce(&mut rusqlite::Connection) -> Result<R> + Send + 'static,
    R: Send + 'static,
  {
    return call_impl(&self.state.reader, function).await;
  }

  pub fn call_and_forget(&self, function: impl FnOnce(&rusqlite::Connection) + Send + 'static) {
    let _ = self
      .state
      .writer
      .send(Message::Run(Box::new(move |conn| function(conn))));
  }

  /// Query SQL statement.
  pub async fn query(&self, sql: &str, params: impl Params + Send + 'static) -> Result<Rows> {
    let sql = sql.to_string();
    return self
      .call(move |conn: &mut rusqlite::Connection| {
        let mut stmt = conn.prepare_cached(&sql)?;
        params.bind(&mut stmt)?;
        let rows = stmt.raw_query();
        Ok(Rows::from_rows(rows)?)
      })
      .await;
  }

  pub async fn query_row(
    &self,
    sql: &str,
    params: impl Params + Send + 'static,
  ) -> Result<Option<Row>> {
    let sql = sql.to_string();
    return self
      .call(move |conn: &mut rusqlite::Connection| {
        let mut stmt = conn.prepare_cached(&sql)?;
        params.bind(&mut stmt)?;
        let mut rows = stmt.raw_query();
        if let Some(row) = rows.next()? {
          return Ok(Some(Row::from_row(row, None)?));
        }
        Ok(None)
      })
      .await;
  }

  pub async fn read_query_row(
    &self,
    sql: &str,
    params: impl Params + Send + 'static,
  ) -> Result<Option<Row>> {
    let sql = sql.to_string();
    return self
      .call_reader(move |conn: &mut rusqlite::Connection| {
        let mut stmt = conn.prepare_cached(&sql)?;
        assert!(stmt.readonly());
        params.bind(&mut stmt)?;

        let mut rows = stmt.raw_query();
        if let Some(row) = rows.next()? {
          return Ok(Some(Row::from_row(row, None)?));
        }
        Ok(None)
      })
      .await;
  }

  pub async fn query_row_f<T: Send + 'static>(
    &self,
    sql: &str,
    params: impl Params + Send + 'static,
    f: impl (FnOnce(&rusqlite::Row<'_>) -> Result<T>) + Send + 'static,
  ) -> Result<Option<T>> {
    let sql = sql.to_string();
    // let lock = self.state.lock.clone();

    return self
      .call(move |conn: &mut rusqlite::Connection| {
        let mut stmt = conn.prepare_cached(&sql)?;
        params.bind(&mut stmt)?;

        let mut rows = {
          // let _write_lock = lock.write();
          stmt.raw_query()
        };

        if let Some(row) = rows.next()? {
          return Ok(Some(f(row)?));
        }
        Ok(None)
      })
      .await;
  }

  pub async fn read_query_row_f<T: Send + 'static>(
    &self,
    sql: &str,
    params: impl Params + Send + 'static,
    f: impl (FnOnce(&rusqlite::Row<'_>) -> Result<T>) + Send + 'static,
  ) -> Result<Option<T>> {
    let sql = sql.to_string();
    // let lock = self.state.lock.clone();

    return self
      .call_reader(move |conn: &mut rusqlite::Connection| {
        let mut stmt = conn.prepare_cached(&sql)?;
        assert!(stmt.readonly());

        params.bind(&mut stmt)?;

        let mut rows = {
          // let _read_lock = lock.read();
          stmt.raw_query()
        };

        if let Some(row) = rows.next()? {
          return Ok(Some(f(row)?));
        }
        Ok(None)
      })
      .await;
  }

  pub async fn query_value<T: serde::de::DeserializeOwned + Send + 'static>(
    &self,
    sql: &str,
    params: impl Params + Send + 'static,
  ) -> Result<Option<T>> {
    let sql = sql.to_string();
    return self
      .call(move |conn: &mut rusqlite::Connection| {
        let mut stmt = conn.prepare_cached(&sql)?;
        params.bind(&mut stmt)?;
        let mut rows = stmt.raw_query();
        if let Some(row) = rows.next()? {
          return Ok(Some(serde_rusqlite::from_row(row)?));
        }
        Ok(None)
      })
      .await;
  }

  pub async fn query_values<T: serde::de::DeserializeOwned + Send + 'static>(
    &self,
    sql: &str,
    params: impl Params + Send + 'static,
  ) -> Result<Vec<T>> {
    let sql = sql.to_string();
    return self
      .call(move |conn: &mut rusqlite::Connection| {
        let mut stmt = conn.prepare_cached(&sql)?;
        params.bind(&mut stmt)?;
        let mut rows = stmt.raw_query();

        let mut values = vec![];
        while let Some(row) = rows.next()? {
          values.push(serde_rusqlite::from_row(row)?);
        }
        return Ok(values);
      })
      .await;
  }

  /// Execute SQL statement.
  pub async fn execute(&self, sql: &str, params: impl Params + Send + 'static) -> Result<usize> {
    let sql = sql.to_string();
    return self
      .call(move |conn: &mut rusqlite::Connection| {
        let mut stmt = conn.prepare_cached(&sql)?;
        params.bind(&mut stmt)?;

        let n = stmt.raw_execute()?;

        return Ok(n);
      })
      .await;
  }

  /// Batch execute SQL statements and return rows of last statement.
  pub async fn execute_batch(&self, sql: &str) -> Result<Option<Rows>> {
    let sql = sql.to_string();
    return self
      .call(move |conn: &mut rusqlite::Connection| {
        let batch = rusqlite::Batch::new(conn, &sql);

        let mut p = batch.peekable();
        while let Ok(Some(mut stmt)) = p.next() {
          let mut rows = stmt.raw_query();
          let row = rows.next()?;

          match p.peek() {
            Err(_) | Ok(None) => {
              if let Some(row) = row {
                let cols: Arc<Vec<Column>> = Arc::new(columns(row.as_ref()));

                let mut result = vec![Row::from_row(row, Some(cols.clone()))?];
                while let Some(row) = rows.next()? {
                  result.push(Row::from_row(row, Some(cols.clone()))?);
                }
                return Ok(Some(Rows(result, cols)));
              }
              return Ok(None);
            }
            _ => {}
          }
        }
        return Ok(None);
      })
      .await;
  }

  /// Convenience API for (un)setting a new pre-update hook.
  pub async fn add_preupdate_hook(
    &self,
    hook: Option<impl (Fn(Action, &str, &str, &PreUpdateCase)) + Send + Sync + 'static>,
  ) -> Result<()> {
    return self
      .call(move |conn| {
        conn.preupdate_hook(hook);
        return Ok(());
      })
      .await;
  }

  /// Close the database connection.
  ///
  /// This is functionally equivalent to the `Drop` implementation for
  /// `Connection`. It consumes the `Connection`, but on error returns it
  /// to the caller for retry purposes.
  ///
  /// If successful, any following `close` operations performed
  /// on `Connection` copies will succeed immediately.
  ///
  /// On the other hand, any calls to [`Connection::call`] will return a
  /// [`Error::ConnectionClosed`], and any calls to [`Connection::call_unwrap`] will cause a
  /// `panic`.
  ///
  /// # Failure
  ///
  /// Will return `Err` if the underlying SQLite close call fails.
  pub async fn close(self) -> Result<()> {
    let (sender, receiver) = oneshot::channel::<std::result::Result<(), rusqlite::Error>>();

    if let Err(crossbeam_channel::SendError(_)) = self.state.writer.send(Message::Close(sender)) {
      // If the channel is closed on the other side, it means the connection closed successfully
      // This is a safeguard against calling close on a `Copy` of the connection
      return Ok(());
    }

    let Ok(result) = receiver.await else {
      // If we get a RecvError at this point, it also means the channel closed in the meantime
      // we can assume the connection is closed
      return Ok(());
    };

    return result.map_err(|e| Error::Close(self, e));
  }
}

impl Debug for Connection {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Connection").finish()
  }
}

fn event_loop(mut conn: rusqlite::Connection, receiver: Receiver<Message>) {
  const BUG_TEXT: &str = "bug in trailbase-sqlite, please report";

  while let Ok(message) = receiver.recv() {
    match message {
      Message::Run(f) => f(&mut conn),
      Message::Close(ch) => {
        match conn.close() {
          Ok(v) => ch.send(Ok(v)).expect(BUG_TEXT),
          Err((_conn, e)) => ch.send(Err(e)).expect(BUG_TEXT),
        };

        return;
      }
    };
  }
}

#[inline]
async fn call_impl<F, R>(s: &Sender<Message>, function: F) -> Result<R>
where
  F: FnOnce(&mut rusqlite::Connection) -> Result<R> + Send + 'static,
  R: Send + 'static,
{
  let (sender, receiver) = oneshot::channel::<Result<R>>();

  s.send(Message::Run(Box::new(move |conn| {
    let value = function(conn);
    let _ = sender.send(value);
  })))
  .map_err(|_| Error::ConnectionClosed)?;

  receiver.await.map_err(|_| Error::ConnectionClosed)?
}

pub fn extract_row_id(case: &PreUpdateCase) -> Option<i64> {
  return match case {
    PreUpdateCase::Insert(accessor) => Some(accessor.get_new_row_id()),
    PreUpdateCase::Delete(accessor) => Some(accessor.get_old_row_id()),
    PreUpdateCase::Update {
      new_value_accessor: accessor,
      ..
    } => Some(accessor.get_new_row_id()),
    PreUpdateCase::Unknown => None,
  };
}

pub fn extract_record_values(case: &PreUpdateCase) -> Option<Vec<Value>> {
  return Some(match case {
    PreUpdateCase::Insert(accessor) => (0..accessor.get_column_count())
      .map(|idx| -> Value {
        accessor
          .get_new_column_value(idx)
          .map_or(rusqlite::types::Value::Null, |v| v.into())
      })
      .collect(),
    PreUpdateCase::Delete(accessor) => (0..accessor.get_column_count())
      .map(|idx| -> rusqlite::types::Value {
        accessor
          .get_old_column_value(idx)
          .map_or(rusqlite::types::Value::Null, |v| v.into())
      })
      .collect(),
    PreUpdateCase::Update {
      new_value_accessor: accessor,
      ..
    } => (0..accessor.get_column_count())
      .map(|idx| -> rusqlite::types::Value {
        accessor
          .get_new_column_value(idx)
          .map_or(rusqlite::types::Value::Null, |v| v.into())
      })
      .collect(),
    PreUpdateCase::Unknown => {
      return None;
    }
  });
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
