//! The host-vitals gate as the claim loops see it. Without the `vitals` feature, or with
//! nothing watched, it always admits.

#[cfg(feature = "vitals")]
use crate::{metrics::Metrics, queue::Queue, vitals::VitalsConfig, vitals::VitalsWatch};

/// Leave to run one claim round; drop it once the claim query returns.
pub struct ClaimPermit {
    /// Always `None` without `vitals`.
    _held: Option<tokio::sync::OwnedRwLockReadGuard<()>>,
}

#[derive(Default)]
pub struct HostAdmission {
    #[cfg(feature = "vitals")]
    watch: Option<VitalsWatch>,
}

impl HostAdmission {
    /// Starts probing when `config` watches anything; the first probe runs before this returns.
    #[cfg(feature = "vitals")]
    pub async fn start(
        config: Option<&VitalsConfig>,
        queue: &Queue,
        worker_id: uuid::Uuid,
        metrics: &Metrics,
    ) -> crate::error::Result<Self> {
        let watch = match config {
            Some(c) if c.is_enabled() => Some(
                VitalsWatch::start(c.clone(), queue.clone(), worker_id, metrics.clone()).await?,
            ),
            _ => None,
        };
        Ok(Self { watch })
    }

    /// Rejects a config that cannot work, before the worker registers.
    #[cfg(feature = "vitals")]
    pub fn check(config: Option<&VitalsConfig>) -> crate::error::Result<()> {
        config.map_or(Ok(()), VitalsConfig::validate)
    }

    /// Whether the next claim may happen.
    #[cfg_attr(
        not(feature = "vitals"),
        expect(
            clippy::unused_self,
            reason = "the gate holds state only with `vitals`"
        )
    )]
    pub fn admitting(&self) -> bool {
        #[cfg(feature = "vitals")]
        {
            self.watch.as_ref().is_none_or(VitalsWatch::admitting)
        }
        #[cfg(not(feature = "vitals"))]
        {
            true
        }
    }

    /// Leave for one claim round, or `None` while paused. Held across the claim query, it
    /// makes a pause wait for the round, so no claim lands after the pause is reported.
    #[cfg_attr(
        not(feature = "vitals"),
        allow(
            clippy::unused_async,
            clippy::unused_async_trait_impl,
            reason = "awaits only with `vitals`; clippy names the lint either way"
        )
    )]
    pub async fn claim_permit(&self) -> Option<ClaimPermit> {
        #[cfg(feature = "vitals")]
        {
            match &self.watch {
                None => Some(ClaimPermit { _held: None }),
                Some(watch) => watch
                    .claim_permit()
                    .await
                    .map(|held| ClaimPermit { _held: Some(held) }),
            }
        }
        #[cfg(not(feature = "vitals"))]
        {
            Some(ClaimPermit { _held: None })
        }
    }

    #[cfg(feature = "vitals")]
    pub fn status(&self) -> Option<crate::vitals::VitalsStatus> {
        self.watch.as_ref().map(VitalsWatch::status)
    }

    #[cfg_attr(
        not(feature = "vitals"),
        expect(
            clippy::unused_self,
            reason = "the gate holds state only with `vitals`"
        )
    )]
    pub fn stop(&self) {
        #[cfg(feature = "vitals")]
        if let Some(watch) = &self.watch {
            watch.stop();
        }
    }
}

/// What a worker listing says about a row's `metadata->'vitals'`: why it claims nothing, or
/// `None` while it claims. Any runtime's worker may have written it.
#[cfg(any(feature = "http", feature = "mcp"))]
pub fn paused_note(vitals: Option<&serde_json::Value>) -> Option<String> {
    let vitals = vitals?;
    if vitals["admitting"] != false {
        return None;
    }
    Some(vitals["reason"].as_str().map_or_else(
        || "claiming paused: host vitals".to_string(),
        |reason| format!("claiming paused: {reason}"),
    ))
}

#[cfg(all(test, any(feature = "http", feature = "mcp")))]
mod tests;
