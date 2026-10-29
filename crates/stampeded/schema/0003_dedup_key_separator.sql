-- Schema 3: the dedup key joins scope/period/bucket as bytea with a raw
-- zero byte. The old ':' join let a crafted dedup_scope (e.g.
-- 'a:hour:1700000000') forge the exact key of an unrelated periodic job
-- (scope 'a', period 'hour', that epoch) -- same sha256, same unique index,
-- wrongly rejected as a duplicate. No `text` value can ever decode to a
-- zero byte (chr(0) itself is refused by Postgres), so the new separator
-- can never be forged.
ALTER TABLE stampeded_jobs ALTER COLUMN dedup_key SET EXPRESSION AS (
    sha256(
        dedup_scope::bytea || '\x00'::bytea || coalesce(
            dedup_period::bytea || '\x00'::bytea || extract(
                epoch FROM (date_trunc(dedup_period, inserted_at, 'UTC') - to_timestamp(0))
            )::bigint::text::bytea,
            ''::bytea
        )
    )
);
