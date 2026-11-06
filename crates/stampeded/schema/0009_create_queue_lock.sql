-- Schema 9: stampeded_create_queue serialises itself. Two callers provisioning
-- the same queue at once used to both pass the existence check, and the
-- second CREATE TABLE failed with "relation already exists". A transaction
-- advisory lock on the partition name makes the check-then-create atomic;
-- different queues still provision in parallel.
CREATE OR REPLACE FUNCTION stampeded_create_queue(p_queue text, p_unlogged boolean DEFAULT false)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    part_name text := 'stampeded_jobs_' || regexp_replace(p_queue, '[^a-zA-Z0-9_]', '_', 'g');
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('stampeded_create_queue'), hashtext(part_name));

    IF to_regclass(part_name) IS NOT NULL THEN
        RETURN;  -- idempotent: provisioning an existing queue is a no-op
    END IF;

    EXECUTE format(
        'CREATE %s TABLE %I PARTITION OF stampeded_jobs FOR VALUES IN (%L)',
        CASE WHEN p_unlogged THEN 'UNLOGGED' ELSE '' END, part_name, p_queue);

    -- Not inherited from the parent. See the note on the parent table.
    EXECUTE format(
        'ALTER TABLE %I SET ('
        '  fillfactor = 85,'
        '  autovacuum_vacuum_scale_factor = 0.02,'
        '  autovacuum_analyze_scale_factor = 0.02,'
        '  autovacuum_vacuum_cost_limit = 2000)',
        part_name);

    -- Direct inserts into a partition bypass a parent-only trigger.
    EXECUTE format(
        'CREATE TRIGGER stampeded_notify_insert_trg'
        '  AFTER INSERT ON %I'
        '  REFERENCING NEW TABLE AS inserted'
        '  FOR EACH STATEMENT EXECUTE FUNCTION stampeded_notify_insert()',
        part_name);

    -- Counters are per-partition for the same reason the notify trigger is.
    PERFORM stampeded_attach_counters(part_name);
END;
$$;

UPDATE stampeded_schema SET version = 9, changed_at = now();
