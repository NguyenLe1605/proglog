use std::sync::{Arc, RwLock};

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use super::log::{Log, LogError, Record};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProduceRequest {
    record: Record,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ProduceResponse {
    offset: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ConsumeRequest {
    offset: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ConsumeResponse {
    record: Record,
}

type RouterState = Arc<RwLock<Log>>;

pub fn create_router() -> Router {
    let log = RouterState::default();
    Router::new()
        .route("/", post(handle_produce))
        .route("/", get(handle_consume))
        .with_state(log)
}

pub async fn handle_produce(
    State(state): State<RouterState>,
    Json(req): Json<ProduceRequest>,
) -> Response {
    let mut log = state.write().expect("posioned write lock");
    let offset = match log.append(req.record) {
        Ok(offset) => offset,
        Err(err) => {
            let body = err.to_string();
            let status = StatusCode::INTERNAL_SERVER_ERROR;
            return (status, body).into_response();
        }
    };
    let resp = ProduceResponse { offset };
    Json(resp).into_response()
}

pub async fn handle_consume(
    State(state): State<RouterState>,
    Json(req): Json<ConsumeRequest>,
) -> Response {
    let log = state.read().expect("poisoned read lock");
    let record = match log.read(req.offset) {
        Ok(record) => record,
        Err(err) => {
            let (status, body) = match err {
                LogError::ErrOffsetNotFound => (StatusCode::NOT_FOUND, err.to_string()),
                LogError::Other => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
            };
            return (status, body).into_response();
        }
    };
    Json(ConsumeResponse { record }).into_response()
}
