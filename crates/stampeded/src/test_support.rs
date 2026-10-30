//! Fixtures shared by the crate's unit tests.

use std::sync::{Arc, Mutex};

// MOCKED: stands in for the embedder's subscriber; keeps each event's level, ignores spans.
#[derive(Default)]
pub struct Levels(Mutex<Vec<tracing::Level>>);

impl Levels {
    /// What `f` returned, and the level of every event it logged, in order.
    pub fn during<T>(f: impl FnOnce() -> T) -> (T, Vec<tracing::Level>) {
        let levels = Arc::new(Self::default());
        let out = tracing::subscriber::with_default(Arc::clone(&levels), f);
        let seen = levels.0.lock().expect("levels poisoned").clone();
        (out, seen)
    }
}

impl tracing::Subscriber for Levels {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        self.0
            .lock()
            .expect("levels poisoned")
            .push(*event.metadata().level());
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
