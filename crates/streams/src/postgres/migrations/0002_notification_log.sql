-- The notification log: one entry per append, in one of 64 partitions fixed here, so a topic's
-- partition stays valid for ever. Drivers take entries with `FOR UPDATE SKIP LOCKED` and ack
-- them with a delete.

CREATE TABLE notifications (
  partition smallint NOT NULL,
  id        bigint GENERATED ALWAYS AS IDENTITY,
  topic_id  bigint NOT NULL,
  position  bigint NOT NULL,
  taken_by  text,
  taken_at  timestamptz,
  PRIMARY KEY (partition, id)
) WITH (fillfactor = 70, autovacuum_vacuum_scale_factor = 0.01);

-- As in migration 1, with the log entry and its doorbell in the append's transaction.
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

-- Keeps the newest `p_keep` entries of each partition, so a log with no driver stays bounded.
CREATE FUNCTION cap_log_v1(p_keep int) RETURNS bigint
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
  v_partition int;
  v_oldest bigint;
  v_rows bigint;
  deleted bigint := 0;
BEGIN
  FOR v_partition IN 0..63 LOOP
    SELECT n.id INTO v_oldest FROM notifications n WHERE n.partition = v_partition
     ORDER BY n.id DESC OFFSET p_keep LIMIT 1;
    IF FOUND THEN
      DELETE FROM notifications n WHERE n.partition = v_partition AND n.id <= v_oldest;
      GET DIAGNOSTICS v_rows = ROW_COUNT;
      deleted := deleted + v_rows;
    END IF;
  END LOOP;
  RETURN deleted;
END $$;
