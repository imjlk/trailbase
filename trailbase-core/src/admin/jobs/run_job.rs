use axum::{extract::State, Json};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

use crate::admin::AdminError as Error;
use crate::AppState;

#[derive(Debug, Deserialize, TS)]
#[ts(export)]
pub struct RunJobRequest {
  id: i32,
}

#[derive(Debug, Serialize, TS)]
#[ts(export)]
pub struct RunJobResponse {
  error: Option<String>,
}

pub async fn run_job_handler(
  State(state): State<AppState>,
  Json(request): Json<RunJobRequest>,
) -> Result<Json<RunJobResponse>, Error> {
  let callback = {
    let jobs = state.jobs();
    let lock = jobs.jobs.lock();
    let Some(task) = lock.get(&request.id) else {
      return Err(Error::Precondition("Not found".into()));
    };

    task.callback.clone()
  };

  let result = callback().await;

  return Ok(Json(RunJobResponse {
    error: result.err().map(|e| e.to_string()),
  }));
}
