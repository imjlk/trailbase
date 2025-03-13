use axum::{extract::State, Json};
use serde::Serialize;
use ts_rs::TS;

use crate::admin::AdminError as Error;
use crate::AppState;

#[derive(Debug, Serialize, TS)]
pub struct Job {
  pub id: i32,
  pub name: String,
  pub schedule: String,
}

#[derive(Debug, Serialize, TS)]
#[ts(export)]
pub struct ListJobsResponse {
  pub jobs: Vec<Job>,
}

pub async fn list_jobs_handler(
  State(state): State<AppState>,
) -> Result<Json<ListJobsResponse>, Error> {
  let jobs: Vec<_> = state
    .jobs()
    .jobs
    .lock()
    .values()
    .map(|t| Job {
      id: t.id,
      name: t.name.clone(),
      schedule: t.schedule.to_string(),
    })
    .collect();

  return Ok(Json(ListJobsResponse { jobs }));
}
