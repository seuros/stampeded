//! Named steps within a job, fast-forwarded on resume.
//!
//! The Rust half of what `ruby/lib/stampede/steps.rb` does, and deliberately
//! the SAME ON-DISK FORMAT — `{"v":1,"completed":[…],"values":{…},"cursors":{…}}`
//! — so a job checkpointed by a Ruby worker can be resumed by a Rust one and
//! the reverse. In a project whose claim is that the workers are peers rather
//! than client and server, two incompatible resume formats would make that
//! claim false in the one place it is most visible.
//!
//! River's model (`resumable.go`) is the prior art: a job declares its phases
//! by name, and on resume the ones already recorded complete are SKIPPED.
//!
//! One departure, shared with the Ruby side: a step's return value is PERSISTED
//! when small and handed back when the step is skipped. River returns nothing,
//! so `let data = step("download", …)` is empty on resume — a job that works
//! until the first crash and then behaves differently.

use crate::error::{Error, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

/// Format version, so a future change can be detected rather than
/// misinterpreted.
pub const VERSION: u32 = 1;

/// Above this, a step result is refused rather than stored. The row cap is
/// 64 KB for everything; one step must not be able to eat it.
pub const MAX_VALUE_BYTES: usize = 8 * 1024;

/// The persisted shape. Matches `Stampede::Steps` in Ruby field for field.
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

// NOT #[derive(Default)]: that gives `v: 0`, because a derived Default uses
// u32::default() and `#[serde(default = "…")]` only applies when DESERIALISING.
// A record written from Rust would then carry version 0, and the first time
// anything checks the version it would misfire. Caught by the round-trip test,
// which is the entire reason it compares against a literal 1.
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
    /// Read the step record out of a job's checkpoint.
    ///
    /// A checkpoint written by the plain cursor API has no `completed` key and
    /// parses to an empty record, which is the right answer: it has no steps.
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
    ///
    /// `None` is the overwhelmingly common case — a step run for its side
    /// effect — and storing it would bloat the record for nothing.
    pub fn complete<T: Serialize>(&mut self, name: &str, value: Option<&T>) -> Result<()> {
        if let Some(v) = value {
            let encoded = serde_json::to_value(v)?;
            let size = serde_json::to_string(&encoded).map(|s| s.len()).unwrap_or(0);
            if size > MAX_VALUE_BYTES {
                return Err(Error::InvalidSchedule(format!(
                    "step {name:?} returned {size} bytes; the limit is {MAX_VALUE_BYTES}. \
                     Step results are stored so a resumed job can skip the step and still see \
                     what it produced, which only works for small values -- an id, a path, a \
                     token. Write the large thing somewhere durable and return a reference."
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
