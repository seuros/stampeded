-- Backslashes are doubled so `::bytea` keeps the scope's own bytes instead
-- of reading escapes: `a` and `\x61` are distinct scopes.
ALTER TABLE stampeded_jobs ALTER COLUMN dedup_key SET EXPRESSION AS (
    sha256(
        replace(dedup_scope, '\', '\\')::bytea || '\x00'::bytea || coalesce(
            dedup_period::bytea || '\x00'::bytea || extract(
                epoch FROM (date_trunc(dedup_period, inserted_at, 'UTC') - to_timestamp(0))
            )::bigint::text::bytea,
            ''::bytea
        )
    )
);
