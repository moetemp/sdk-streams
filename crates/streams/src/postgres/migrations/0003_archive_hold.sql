-- The archive's hold: a `floors` row with the holder `archive`, made at a topic's first record and
-- moved by the archive, and `archive_pending`, the archive's work list.

-- As in migration 2, with the archive's hold made at a topic's first record, so retention can't
-- drop a record before the archive's first pass.
CREATE OR REPLACE FUNCTION append_v1(
  p_namespace text, p_kind smallint, p_owner text, p_run text, p_activity text,
  p_topic text, p_partition smallint, p_producer text, p_attempt bigint, p_sequence bigint,
  p_digest bytea, p_records bytea[], p_keeps_topics boolean, p_log boolean, p_doorbells int,
  p_archive_lease_ms bigint, p_retention_ms bigint, p_notify_reads boolean,
  OUT status smallint, OUT first_position bigint, OUT last_position bigint,
  OUT next_sequence bigint, OUT close bytea)
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
  v_chain bigint;
  v_chain_closed boolean;
  v_topic topics%ROWTYPE;
  v_held producers%ROWTYPE;
  v_count int := cardinality(p_records);
  v_now timestamptz;
  v_last bigint;
BEGIN
  SELECT l.chain_id, l.closed INTO v_chain, v_chain_closed
    FROM live_chain_v1(p_namespace, p_kind, p_owner, p_run, p_activity) l;
  LOOP
    SELECT * INTO v_topic FROM topics t
     WHERE t.chain_id = v_chain AND t.topic = p_topic FOR NO KEY UPDATE;
    EXIT WHEN FOUND;
    -- Checked before the topic exists, so a refused batch leaves no trace of it.
    IF v_chain_closed THEN
      status := 4;
      RETURN;
    END IF;
    PERFORM topic_id_v1(v_chain, p_topic, p_partition);
  END LOOP;
  SELECT * INTO v_held FROM producers h
   WHERE h.topic_id = v_topic.topic_id AND h.producer_id = p_producer
     AND h.attempt = p_attempt;
  IF FOUND THEN
    IF p_sequence = v_held.sequence THEN
      IF p_digest = v_held.digest THEN
        status := 1;
        first_position := v_held.first_position;
        last_position := v_held.first_position + v_held.count - 1;
      ELSE
        status := 2;
      END IF;
      RETURN;
    END IF;
    IF p_sequence < v_held.sequence + v_held.count THEN
      status := 3;
      next_sequence := v_held.sequence + v_held.count;
      RETURN;
    END IF;
  END IF;
  IF v_chain_closed OR v_topic.closed THEN
    status := 4;
    close := v_topic.close;
    RETURN;
  END IF;
  IF p_archive_lease_ms IS NOT NULL AND v_topic.last_position IS NULL THEN
    INSERT INTO floors
    VALUES (v_topic.topic_id, 'archive', 0, now() + p_archive_lease_ms * interval '1 millisecond')
    ON CONFLICT DO NOTHING;
  END IF;
  -- Taken under the row lock, so the times grow with the positions.
  v_now := clock_timestamp();
  v_last := coalesce(v_topic.last_position, v_topic.base);
  INSERT INTO records (topic_id, position, written_at, record)
  SELECT v_topic.topic_id, v_last + u.i, v_now, u.r
    FROM unnest(p_records) WITH ORDINALITY AS u(r, i);
  INSERT INTO producers VALUES (v_topic.topic_id, p_producer, p_attempt, p_sequence, v_count,
                                v_last + 1, p_digest, v_now)
  ON CONFLICT (topic_id, producer_id, attempt) DO UPDATE
    SET sequence = excluded.sequence, count = excluded.count,
        first_position = excluded.first_position, digest = excluded.digest,
        written_at = excluded.written_at;
  UPDATE topics t SET
    last_position = v_last + v_count,
    written_at = v_now,
    written = t.written OR (p_keeps_topics AND p_sequence = 1),
    archive_pending = t.archive_pending OR p_archive_lease_ms IS NOT NULL,
    trim_at = coalesce(t.trim_at, v_now + p_retention_ms * interval '1 millisecond')
  WHERE t.topic_id = v_topic.topic_id;
  IF p_log THEN
    -- A busy partition rings no more, so rings stay rare under load.
    IF NOT EXISTS (SELECT 1 FROM notifications n WHERE n.partition = v_topic.partition) THEN
      PERFORM pg_notify(current_schema() || '_bell_' || (v_topic.partition % p_doorbells),
                        v_topic.partition::text);
    END IF;
    INSERT INTO notifications (partition, topic_id, position)
    VALUES (v_topic.partition, v_topic.topic_id, v_last + v_count);
  END IF;
  IF p_notify_reads THEN
    PERFORM pg_notify(current_schema() || '_read_' || (v_topic.topic_id % 64),
                      v_topic.topic_id::text);
  END IF;
  status := 0;
  first_position := v_last + 1;
  last_position := v_last + v_count;
END $$;

-- As in migration 1. A close with `p_archive` puts the topic on the archive's work list, first,
-- since a closed topic gets nothing new.
CREATE OR REPLACE FUNCTION close_topic_v1(p_namespace text, p_kind smallint, p_owner text,
  p_run text, p_activity text, p_topic text, p_partition smallint, p_close bytea,
  p_archive boolean)
RETURNS bytea
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
  v_chain bigint;
  v_topic bigint;
  v_close bytea;
BEGIN
  SELECT l.chain_id INTO v_chain
    FROM live_chain_v1(p_namespace, p_kind, p_owner, p_run, p_activity) l;
  LOOP
    v_topic := topic_id_v1(v_chain, p_topic, p_partition);
    UPDATE topics t SET closed = true, close = coalesce(t.close, p_close),
           archive_pending = t.archive_pending OR p_archive
     WHERE t.topic_id = v_topic
    RETURNING t.close INTO v_close;
    EXIT WHEN FOUND;
  END LOOP;
  RETURN v_close;
END $$;

-- As in migration 1. The archive releases once it holds a closed topic to its end, so the topic
-- also leaves the archive's work list.
CREATE OR REPLACE FUNCTION release_floor_v1(p_namespace text, p_kind smallint, p_owner text,
  p_run text, p_activity text, p_topic text, p_holder text, p_grace_ms bigint) RETURNS void
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
  UPDATE floors f SET until = least(f.until, now() + p_grace_ms * interval '1 millisecond')
    FROM chains c JOIN topics t ON t.chain_id = c.chain_id AND t.topic = p_topic
   WHERE (c.namespace, c.owner_kind, c.owner_id, c.first_run_id, c.activity_id)
       = (p_namespace, p_kind, p_owner, p_run, p_activity)
     AND f.topic_id = t.topic_id AND f.holder = p_holder;
  IF p_holder = 'archive' THEN
    UPDATE topics t SET archive_pending = false FROM chains c
     WHERE (c.namespace, c.owner_kind, c.owner_id, c.first_run_id, c.activity_id)
         = (p_namespace, p_kind, p_owner, p_run, p_activity)
       AND t.chain_id = c.chain_id AND t.topic = p_topic;
  END IF;
END $$;

-- Moves the archive's hold to `p_position` with no minimum, renews its lease and keeps the
-- position as the topic's `archived`. A topic with no row stays without one.
CREATE FUNCTION advance_archive_v1(p_namespace text, p_kind smallint, p_owner text, p_run text,
  p_activity text, p_topic text, p_position bigint, p_lease_ms bigint) RETURNS void
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
  v_topic bigint;
BEGIN
  -- The key share lock keeps the janitor from deleting the topic under the insert.
  SELECT t.topic_id INTO v_topic FROM chains c
    JOIN topics t ON t.chain_id = c.chain_id AND t.topic = p_topic
   WHERE (c.namespace, c.owner_kind, c.owner_id, c.first_run_id, c.activity_id)
       = (p_namespace, p_kind, p_owner, p_run, p_activity)
     FOR KEY SHARE OF t;
  IF NOT FOUND THEN
    RETURN;
  END IF;
  INSERT INTO floors AS f
  VALUES (v_topic, 'archive', p_position, now() + p_lease_ms * interval '1 millisecond')
  ON CONFLICT (topic_id, holder) DO UPDATE
    SET position = excluded.position, until = excluded.until;
  UPDATE topics t SET archived = p_position,
         archive_pending = t.archive_pending AND t.last_position IS DISTINCT FROM p_position
   WHERE t.topic_id = v_topic;
END $$;
