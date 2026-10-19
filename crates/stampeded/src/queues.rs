//! Which queues a worker serves.

/// Which queues a worker serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Queues {
    /// These queues, claimed from in turn.
    Named(Vec<String>),
    /// Every queue that has claimable work, except these.
    All { except: Vec<String> },
}

impl Default for Queues {
    fn default() -> Self {
        Self::one("default")
    }
}

impl Queues {
    pub fn one(name: impl Into<String>) -> Self {
        Self::Named(vec![name.into()])
    }

    /// Parses `"mail"`, `"default,mail"` or `"*"`; a blank entry is an error, not skipped.
    pub fn parse(spec: &str) -> Result<Self, String> {
        let spec = spec.trim();
        if spec == "*" {
            return Ok(Self::All { except: Vec::new() });
        }

        let names = names(spec)?;
        if names.iter().any(|n| n == "*") {
            return Err(format!(
                "{spec:?}: \"*\" cannot be mixed with queue names; leave queues out with except"
            ));
        }
        Ok(Self::Named(names))
    }

    /// The same selection with these queues left out. Only `*` takes them.
    pub fn except(self, spec: &str) -> Result<Self, String> {
        match self {
            Self::All { .. } => Ok(Self::All {
                except: names(spec)?,
            }),
            Self::Named(_) => Err("except applies only to every queue (\"*\")".into()),
        }
    }

    /// Whether a job in `queue` is this worker's to claim.
    pub fn serves(&self, queue: &str) -> bool {
        match self {
            Self::Named(names) => names.iter().any(|n| n == queue),
            Self::All { except } => !except.iter().any(|n| n == queue),
        }
    }

    /// What `stampeded_workers.queues` records: the names, or `*`.
    pub fn registered(&self) -> Vec<String> {
        match self {
            Self::Named(names) => names.clone(),
            Self::All { .. } => vec!["*".into()],
        }
    }

    /// One label for metrics and logs.
    pub fn label(&self) -> String {
        self.registered().join(",")
    }
}

fn names(spec: &str) -> Result<Vec<String>, String> {
    let mut names: Vec<String> = Vec::new();
    for name in spec.split(',').map(str::trim) {
        if name.is_empty() {
            return Err(format!("queue list {spec:?} has a blank entry"));
        }
        if !names.iter().any(|n| n == name) {
            names.push(name.to_string());
        }
    }
    Ok(names)
}

#[cfg(test)]
mod tests;
