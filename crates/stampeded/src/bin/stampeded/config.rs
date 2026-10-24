//! Daemon configuration, parsed by hand.

use stampeded::Queues;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub queues: Queues,
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
    pub dashboard_token: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            database_url: String::new(),
            queues: Queues::default(),
            concurrency: 10,
            batch_size: 5,
            // Below `concurrency`: a connection is never held for a job's duration.
            pool_size: 5,
            poll_interval: Duration::from_secs(1),
            job_timeout: Duration::from_mins(15),
            dead_after: Duration::from_secs(300),
            retain_for: Duration::from_hours(14 * 24),
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
            "  --queue SPEC           queues to work: a name, a,b, or * (default: default)\n",
            "  --except-queues A,B    with --queue *, queues to leave alone\n",
            "  --concurrency N        max jobs in flight (default: 10)\n",
            "  --batch-size N         jobs claimed per round trip (default: 5)\n",
            "  --pool-size N          Postgres connections (default: 5)\n",
            "  --poll-interval SECS   claim poll floor (default: 1)\n",
            "  --job-timeout SECS     per-job execution timeout (default: 900)\n",
            "  --dead-after SECS      reclaim jobs from workers silent this long (default: 300)\n",
            "  --retain-for SECS      keep finished jobs this long (default: 1209600)\n",
            "  --no-worker            do not claim jobs (maintenance only)\n",
            "  --no-maintenance       do not reap or prune\n",
            "  --migrate              install or upgrade the schema on startup (safe on every start)\n",
            "  --http ADDR            serve the HTTP API on ADDR (e.g. 127.0.0.1:7878)\n",
            "  --dashboard-token TOK  serve the dashboard, gated on this token\n",
            "                         (or STAMPEDED_DASHBOARD_TOKEN; without either\n",
            "                         the dashboard is NOT served)\n",
            "  -h, --help             this message\n",
            "  -V, --version          print the version and exit\n",
        )
    }

    pub fn from_args_and_env() -> Result<Self, String> {
        Self::parse(std::env::args().skip(1).collect(), |k| {
            std::env::var(k).ok()
        })
    }

    /// Parse `args` with `env` standing in for the process environment.
    pub fn parse(args: Vec<String>, env: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let mut c = Self {
            database_url: env("STAMPEDED_DATABASE_URL").unwrap_or_default(),
            // Empty means unset: an empty token would open the dashboard to `?token=`.
            dashboard_token: env("STAMPEDED_DASHBOARD_TOKEN").filter(|t| !t.is_empty()),
            ..Default::default()
        };
        let mut queues = env("STAMPEDED_QUEUE").unwrap_or_else(|| "default".into());
        let mut except: Option<String> = None;

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
                "-V" | "--version" => {
                    println!("stampeded {}", env!("CARGO_PKG_VERSION"));
                    std::process::exit(0);
                }
                "--database-url" => c.database_url = value("--database-url")?,
                "--queue" => queues = value("--queue")?,
                "--except-queues" => except = Some(value("--except-queues")?),
                "--concurrency" => {
                    c.concurrency = parse_num(&value("--concurrency")?, "--concurrency")?;
                }
                "--batch-size" => {
                    c.batch_size = parse_num(&value("--batch-size")?, "--batch-size")?;
                }
                "--pool-size" => c.pool_size = parse_num(&value("--pool-size")?, "--pool-size")?,
                "--poll-interval" => {
                    c.poll_interval = parse_secs(&value("--poll-interval")?, "--poll-interval")?;
                }
                "--job-timeout" => {
                    c.job_timeout = parse_secs(&value("--job-timeout")?, "--job-timeout")?;
                }
                "--dead-after" => {
                    c.dead_after = parse_secs(&value("--dead-after")?, "--dead-after")?;
                }
                "--retain-for" => {
                    c.retain_for = parse_secs(&value("--retain-for")?, "--retain-for")?;
                }
                "--no-worker" => c.worker = false,
                "--no-maintenance" => c.maintenance = false,
                "--migrate" => c.migrate = true,
                "--http" => c.http = Some(value("--http")?),
                "--dashboard-token" => c.dashboard_token = Some(value("--dashboard-token")?),
                other => return Err(format!("unknown option {other:?}")),
            }
        }

        c.queues = Queues::parse(&queues).map_err(|e| format!("--queue: {e}"))?;
        if let Some(except) = except {
            c.queues = c
                .queues
                .except(&except)
                .map_err(|e| format!("--except-queues: {e}"))?;
        }

        c.validate()?;
        Ok(c)
    }

    fn validate(&self) -> Result<(), String> {
        if self.database_url.is_empty() {
            return Err("--database-url or STAMPEDED_DATABASE_URL is required".into());
        }
        if self.dashboard_token.as_deref() == Some("") {
            return Err("--dashboard-token must not be empty".into());
        }
        if self.concurrency == 0 {
            return Err("--concurrency must be at least 1".into());
        }
        if self.batch_size < 1 {
            return Err("--batch-size must be at least 1".into());
        }

        // dead_after must clear WorkerConfig's 30s heartbeat, or live jobs get reclaimed.
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
