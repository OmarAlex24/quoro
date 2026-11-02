//! Shared pagination (`Limit` and `Cursor` parameters in `openapi.yaml`).
//!
//! Rows are ordered by `id` ascending; ULIDs sort by creation time, so the
//! order is creation order. The cursor is the last id of the previous page.

use crate::error::ApiError;
use serde::Deserialize;

pub const DEFAULT_LIMIT: i64 = 50;
pub const MAX_LIMIT: i64 = 500;

/// `limit` and `cursor` query parameters.
#[derive(Debug, Default, Deserialize)]
pub struct PageQuery {
    pub limit: Option<i64>,
    pub cursor: Option<String>,
}

impl PageQuery {
    /// The validated limit.
    ///
    /// # Errors
    ///
    /// `validation_failed` outside 1-500.
    pub fn limit(&self) -> Result<i64, ApiError> {
        match self.limit {
            None => Ok(DEFAULT_LIMIT),
            Some(n) if (1..=MAX_LIMIT).contains(&n) => Ok(n),
            Some(n) => Err(ApiError::validation(format!(
                "limit {n} is outside 1-{MAX_LIMIT}"
            ))),
        }
    }

    /// The cursor, or the empty string, which sorts before every id.
    #[must_use]
    pub fn after(&self) -> &str {
        self.cursor.as_deref().unwrap_or("")
    }
}

/// Splits `rows`, fetched with `LIMIT limit + 1`, into the page and its
/// `next_cursor`.
pub fn finish<T>(
    mut rows: Vec<T>,
    limit: i64,
    id: impl Fn(&T) -> String,
) -> (Vec<T>, Option<String>) {
    let limit = usize::try_from(limit).unwrap_or(0);
    if rows.len() > limit {
        rows.truncate(limit);
        let next = rows.last().map(id);
        (rows, next)
    } else {
        (rows, None)
    }
}
