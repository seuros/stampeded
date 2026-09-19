//! Daemon configuration.
//!
//! Hand-rolled rather than clap: the flag set is small and stable, and a
//! dependency that pulls in a derive macro chain for eight options is not worth
//! it in a binary whose whole point is being easy to deploy.

use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub queue: String,
    pub concurrency: usize,
    pub batch_size: i32,
    pub pool_size: u32,
    pub poll_interval: Duration,
    pub job_timeout: Duration,
    pub dead_after: Duration,
    pub retain_for: Duration,
    pub worker: bool,
    pub maintenance: bool,
    pub migrate: bool,
    /// `Some(addr)` enables the HTTP edge.
    pub http: Option<String>,
    /// Shared secret for the dashboard. Without it the dashboard is not served.
    ///
    /// Opt-OUT rather than opt-in, deliberately: plans/06 names the precedent,
    /// where an opt-in dashboard auth setting meant the dashboard sat open on
    /// staging and production for a long time because nobody opted in.
    pub dashboard_token: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            database_url: String::new(),
            queue: "default".into(),
            concurrency: 10,
            batch_size: 5,
            // Deliberately much smaller than `concurrency`: a connection is held
            // only while claiming or reporting, never for a job's duration.
            pool_size: 5,
            poll_interval: Duration::from_secs(1),
            job_timeout: Duration::from_secs(15 * 60),
            dead_after: Duration::from_secs(300),
            retain_for: Duration::from_secs(14 * 24 * 60 * 60),
            worker: true,
            maintenance: true,
            migrate: false,
            http: None,
            dashboard_token: None,
        }
    }
}

impl Config {
    pub fn usage() -> &'static str {
        concat!(
            "usage: stampeded [options]\n",
            "\n",
            "  --database-url URL     Postgres URL (or STAMPEDED_DATABASE_URL)\n",
            "  --queue NAME           queue to work (default: default)\n",
            "  --concurrency N        max jobs in flight (default: 10)\n",
            "  --batch-size N         jobs claimed per round trip (default: 5)\n",
            "  --pool-size N          Postgres connections (default: 5)\n",
            "  --poll-interval SECS   claim poll floor (default: 1)\n",
            "  --job-timeout SECS     per-job execution timeout (default: 900)\n",
            "  --dead-after SECS      reclaim jobs from workers silent this long (default: 300)\n",
            "  --retain-for SECS      keep finished jobs this long (default: 1209600)\n",
            "  --no-worker            do not claim jobs (maintenance only)\n",
            "  --no-maintenance       do not reap or prune\n",
            "  --migrate              apply the schema on startup\n",
            "  --http ADDR            serve the HTTP API on ADDR (e.g. 127.0.0.1:7878)\n",
            "  --dashboard-token TOK  serve the dashboard, gated on this token\n",
            "                         (without it the dashboard is NOT served)\n",
            "  -h, --help             this message\n",
            "  -V, --version          print the version and exit\n",
        )
    }

    pub fn from_args_and_env() -> Result<Self, String> {
        Self::parse(std::env::args().skip(1).collect(), |k| {
            std::env::var(k).ok()
        })
    }

    /// Split out from `from_args_and_env` so it is testable without touching
    /// the process environment.
    pub fn parse(args: Vec<String>, env: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let mut c = Config {
            database_url: env("STAMPEDED_DATABASE_URL").unwrap_or_default(),
            ..Default::default()
        };
        if let Some(q) = env("STAMPEDED_QUEUE") {
            c.queue = q;
        }

        let mut it = args.into_iter();
        while let Some(arg) = it.next() {
            let mut value = |name: &str| -> Result<String, String> {
                it.next().ok_or_else(|| format!("{name} requires a value"))
            };

            match arg.as_str() {
                "-h" | "--help" => {
                    println!("{}", Self::usage());
                    std::process::exit(0);
                }
                // Every daemon has this, and the first thing anyone does when
                // a deployment misbehaves is ask what is actually running.
                // Without it the answer was "read the HTTP health endpoint",
                // which needs the daemon to be working.
                "-V" | "--version" => {
                    println!("stampeded {}", env!("CARGO_PKG_VERSION"));
                    std::process::exit(0);
                }
                "--database-url" => c.database_url = value("--database-url")?,
                "--queue" => c.queue = value("--queue")?,
                "--concurrency" => {
                    c.concurrency = parse_num(&value("--concurrency")?, "--concurrency")?
                }
                "--batch-size" => {
                    c.batch_size = parse_num(&value("--batch-size")?, "--batch-size")?
                }
                "--pool-size" => c.pool_size = parse_num(&value("--pool-size")?, "--pool-size")?,
                "--poll-interval" => {
                    c.poll_interval = parse_secs(&value("--poll-interval")?, "--poll-interval")?
                }
                "--job-timeout" => {
                    c.job_timeout = parse_secs(&value("--job-timeout")?, "--job-timeout")?
                }
                "--dead-after" => {
                    c.dead_after = parse_secs(&value("--dead-after")?, "--dead-after")?
                }
                "--retain-for" => {
                    c.retain_for = parse_secs(&value("--retain-for")?, "--retain-for")?
                }
                "--no-worker" => c.worker = false,
                "--no-maintenance" => c.maintenance = false,
                "--migrate" => c.migrate = true,
                "--http" => c.http = Some(value("--http")?),
                "--dashboard-token" => c.dashboard_token = Some(value("--dashboard-token")?),
                other => return Err(format!("unknown option {other:?}")),
            }
        }

        c.validate()?;
        Ok(c)
    }

    fn validate(&self) -> Result<(), String> {
        if self.database_url.is_empty() {
            return Err("--database-url or STAMPEDED_DATABASE_URL is required".into());
        }
        if self.concurrency == 0 {
            return Err("--concurrency must be at least 1".into());
        }
        if self.batch_size < 1 {
            return Err("--batch-size must be at least 1".into());
        }

        // The invariant that matters most: a worker whose heartbeat interval is
        // not comfortably inside dead_after gets its own in-flight jobs
        // reclaimed underneath it, which looks like random duplicate execution.
        // WorkerConfig's default heartbeat is 30s.
        let heartbeat = Duration::from_secs(30);
        if self.dead_after < heartbeat * 3 {
            return Err(format!(
                "--dead-after {}s is too tight against the {}s heartbeat; \
                 healthy workers would be reaped mid-job (use at least {}s)",
                self.dead_after.as_secs(),
                heartbeat.as_secs(),
                (heartbeat * 3).as_secs()
            ));
        }

        Ok(())
    }
}

fn parse_num<T: std::str::FromStr>(v: &str, name: &str) -> Result<T, String> {
    v.parse()
        .map_err(|_| format!("{name}: {v:?} is not a number"))
}

fn parse_secs(v: &str, name: &str) -> Result<Duration, String> {
    Ok(Duration::from_secs(parse_num::<u64>(v, name)?))
}

#[cfg(test)]
mod tests;
