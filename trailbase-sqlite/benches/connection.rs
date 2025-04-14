use crate::error::BenchmarkError;
use parking_lot::Mutex;
use rusqlite::types::{FromSql, ToSql, Value};
use trailbase_sqlite::Connection;

pub trait AsyncConnection: Send + Sync {
  fn async_query<T: FromSql + Send + 'static>(
    &self,
    sql: impl Into<String> + Send,
    params: impl Into<Vec<Value>> + Send,
  ) -> impl std::future::Future<Output = Result<T, BenchmarkError>> + Send;

  fn async_read_query<T: FromSql + Send + 'static>(
    &self,
    sql: impl Into<String> + Send,
    params: impl Into<Vec<Value>> + Send,
  ) -> impl std::future::Future<Output = Result<T, BenchmarkError>> + Send {
    return self.async_query(sql, params);
  }

  fn async_execute(
    &self,
    sql: impl Into<String> + Send,
    params: impl Into<Vec<Value>> + Send,
  ) -> impl std::future::Future<Output = Result<(), BenchmarkError>> + Send;
}

impl AsyncConnection for Connection {
  async fn async_query<T: FromSql + Send + 'static>(
    &self,
    sql: impl Into<String> + Send,
    params: impl Into<Vec<Value>> + Send,
  ) -> Result<T, BenchmarkError> {
    return Ok(
      self
        .query_row_f(sql.into(), params.into(), |row| row.get::<_, T>(0))
        .await?
        .unwrap(),
    );
  }

  async fn async_read_query<T: FromSql + Send + 'static>(
    &self,
    sql: impl Into<String> + Send,
    params: impl Into<Vec<Value>> + Send,
  ) -> Result<T, BenchmarkError> {
    return Ok(
      self
        .read_query_row_f(sql.into(), params.into(), |row| row.get::<_, T>(0))
        .await?
        .unwrap(),
    );
  }

  async fn async_execute(
    &self,
    sql: impl Into<String> + Send,
    params: impl Into<Vec<Value>> + Send,
  ) -> Result<(), BenchmarkError> {
    self.execute(sql.into(), params.into()).await?;
    return Ok(());
  }
}

/// Only meant for reference. This implementation is ill-suited since it can clog-up the tokio
/// runtime with sync sqlite calls.
pub struct SharedRusqlite(pub Mutex<rusqlite::Connection>);

impl AsyncConnection for SharedRusqlite {
  async fn async_query<T: FromSql + Send + 'static>(
    &self,
    sql: impl Into<String> + Send,
    params: impl Into<Vec<Value>> + Send,
  ) -> Result<T, BenchmarkError> {
    let params: Vec<Value> = params.into();
    let p: Vec<&dyn ToSql> = params.iter().map(|v| v as &dyn ToSql).collect();

    return Ok(
      self
        .0
        .lock()
        .query_row(&sql.into(), p.as_slice(), |row| row.get::<_, T>(0))?,
    );
  }

  async fn async_execute(
    &self,
    sql: impl Into<String> + Send,
    params: impl Into<Vec<Value>> + Send,
  ) -> Result<(), BenchmarkError> {
    let params: Vec<Value> = params.into();
    let p: Vec<&dyn ToSql> = params.iter().map(|v| v as &dyn ToSql).collect();

    self.0.lock().execute(&sql.into(), p.as_slice())?;

    return Ok(());
  }
}

/// Only meant for reference. This implementation is ill-suited since it can clog-up the tokio
/// runtime with sync sqlite calls.
/// Additionally, the simple thread_local setup only allows for one connection at the time.
pub struct ThreadLocalRusqlite(
  pub Box<dyn (Fn() -> rusqlite::Connection) + Send + Sync>,
  pub u64,
);

impl ThreadLocalRusqlite {
  #[inline]
  fn call<T>(
    &self,
    f: impl FnOnce(&mut rusqlite::Connection) -> rusqlite::Result<T>,
  ) -> rusqlite::Result<T> {
    use std::cell::{OnceCell, RefCell};
    thread_local! {
      static CELL : OnceCell<RefCell<(rusqlite::Connection, u64)>> = OnceCell::new();
    }

    return CELL.with(|cell| {
      fn init(s: &ThreadLocalRusqlite) -> (rusqlite::Connection, u64) {
        return (s.0(), s.1);
      }

      let ref_cell = cell.get_or_init(|| RefCell::new(init(self)));
      {
        let (conn, id): &mut (rusqlite::Connection, u64) = &mut ref_cell.borrow_mut();
        if *id == self.1 {
          return f(conn);
        }
      }

      // Reinitialize: new benchmark run with different DB folder.
      ref_cell.replace(init(self));
      let (conn, _): &mut (rusqlite::Connection, u64) = &mut ref_cell.borrow_mut();
      return f(conn);
    });
  }
}

impl AsyncConnection for ThreadLocalRusqlite {
  async fn async_query<T: FromSql + Send + 'static>(
    &self,
    sql: impl Into<String> + Send,
    params: impl Into<Vec<Value>> + Send,
  ) -> Result<T, BenchmarkError> {
    let params: Vec<Value> = params.into();
    let p: Vec<&dyn ToSql> = params.iter().map(|v| v as &dyn ToSql).collect();

    return Ok(self.call(move |conn| {
      return Ok(conn.query_row(&sql.into(), p.as_slice(), |row| row.get::<_, T>(0))?);
    })?);
  }

  async fn async_execute(
    &self,
    sql: impl Into<String> + Send,
    params: impl Into<Vec<Value>> + Send,
  ) -> Result<(), BenchmarkError> {
    let params: Vec<Value> = params.into();
    let p: Vec<&dyn ToSql> = params.iter().map(|v| v as &dyn ToSql).collect();

    self.call(move |conn| conn.execute(&sql.into(), p.as_slice()))?;
    return Ok(());
  }
}
