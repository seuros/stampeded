//! Named steps within a job, fast-forwarded on resume.
//!
//! Same on-disk format as `ruby/lib/stampeded/steps.rb`: either worker resumes the other's job.
//! A small step result is persisted and handed back when the step is skipped.

use crate::error::{Error, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// Format version.
pub const VERSION: u32 = 1;

/// Largest step result stored, in bytes; the whole checkpoint is capped at 64 KB.
pub const MAX_VALUE_BYTES: usize = 8 * 1024;

/// The persisted shape. Matches `Stampeded::Steps` in Ruby field for field.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct StepState {
    #[serde(default = "default_version")]
    pub v: u32,
    #[serde(default)]
    pub completed: BTreeSet<String>,
    #[serde(default)]
    pub values: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub cursors: BTreeMap<String, serde_json::Value>,
}

fn default_version() -> u32 {
    VERSION
}

// Not derived: that gives `v: 0`; `#[serde(default)]` only applies when deserializing.
impl Default for StepState {
    fn default() -> Self {
        Self {
            v: VERSION,
            completed: BTreeSet::new(),
            values: BTreeMap::new(),
            cursors: BTreeMap::new(),
        }
    }
}

impl StepState {
    /// Read the step record out of a job's checkpoint; a plain cursor checkpoint has none.
    pub fn from_checkpoint(checkpoint: Option<&serde_json::Value>) -> Self {
        checkpoint
            .filter(|c| c.get("completed").is_some())
            .and_then(|c| serde_json::from_value(c.clone()).ok())
            .unwrap_or_default()
    }

    pub fn completed(&self, name: &str) -> bool {
        self.completed.contains(name)
    }

    /// The value a completed step produced, if it stored one.
    pub fn value<T: DeserializeOwned>(&self, name: &str) -> Option<T> {
        serde_json::from_value(self.values.get(name)?.clone()).ok()
    }

    /// Position within a step that has not finished.
    pub fn cursor<T: DeserializeOwned>(&self, name: &str) -> Option<T> {
        serde_json::from_value(self.cursors.get(name)?.clone()).ok()
    }

    /// Record a step as done, with whatever it produced.
    pub fn complete<T: Serialize>(&mut self, name: &str, value: Option<&T>) -> Result<()> {
        if let Some(v) = value {
            let encoded = serde_json::to_value(v)?;
            let size = serde_json::to_string(&encoded).map_or(0, |s| s.len());
            if size > MAX_VALUE_BYTES {
                return Err(Error::InvalidSchedule(format!(
                    "step {name:?} returned {size} bytes; the limit is {MAX_VALUE_BYTES}. \
                     Store the result somewhere durable and return a reference to it."
                )));
            }
            self.values.insert(name.to_string(), encoded);
        }
        self.completed.insert(name.to_string());
        self.cursors.remove(name);
        Ok(())
    }

    pub fn set_cursor<T: Serialize>(&mut self, name: &str, cursor: &T) -> Result<()> {
        self.cursors
            .insert(name.to_string(), serde_json::to_value(cursor)?);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
