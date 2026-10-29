-- Schema 2: the dedup key names its period. Without it, an hourly and a
-- daily unique job of the same scope shared a key whenever their buckets
-- started together (every midnight UTC, Mondays, the 1st of a month) and
-- refused each other. The new key is strictly finer: no row can collide.
ALTER TABLE stampeded_jobs ALTER COLUMN dedup_key SET EXPRESSION AS (
    sha256((
        dedup_scope || coalesce(
            ':' || dedup_period || ':' || extract(
                epoch FROM (date_trunc(dedup_period, inserted_at, 'UTC') - to_timestamp(0))
            )::bigint::text,
            ''
        )
    )::bytea)
);
