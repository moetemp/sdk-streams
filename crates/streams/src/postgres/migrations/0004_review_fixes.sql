-- Fixes after review: retention, rings, the log cap, the holder cap and the archive's hold.

-- As in migration 3. Every batch puts a Workflow Activity's topic in its set, since a first batch
-- can be lost. The doorbell rings unless the partition holds an entry no driver took.
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
    written = t.written OR p_keeps_topics,
    archive_pending = t.archive_pending OR p_archive_lease_ms IS NOT NULL,
    trim_at = coalesce(t.trim_at, v_now + p_retention_ms * interval '1 millisecond')
  WHERE t.topic_id = v_topic.topic_id;
  IF p_log THEN
    -- A partition with entries no driver took yet rings no more, so rings stay rare under load.
    -- An entry a driver took doesn't count, since the driver may have finished its drain.
    IF NOT EXISTS (SELECT 1 FROM notifications n
                    WHERE n.partition = v_topic.partition AND n.taken_by IS NULL) THEN
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

-- As in migration 1. A topic whose hold keeps its oldest record is due again when a hold ends,
-- not at once, so held topics don't take every janitor pass.
CREATE OR REPLACE FUNCTION trim_topic_v1(p_topic bigint, p_retention_ms bigint, p_grace_ms bigint,
  p_batch int, OUT deleted bigint, OUT done boolean)
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
  t topics%ROWTYPE;
  v_retention interval := p_retention_ms * interval '1 millisecond';
  v_grace interval := p_grace_ms * interval '1 millisecond';
  v_expired timestamptz := now() - v_retention;
  v_keep bigint;
  v_cut bigint;
  v_floor bigint;
  v_floor_until timestamptz;
  v_limit bigint;
  v_oldest timestamptz;
  v_held boolean;
BEGIN
  deleted := 0;
  SELECT * INTO t FROM topics WHERE topic_id = p_topic FOR UPDATE SKIP LOCKED;
  IF NOT FOUND THEN done := false; RETURN; END IF;
  DELETE FROM floors f WHERE f.topic_id = p_topic AND f.until <= now();
  SELECT min(f.position), min(f.until) INTO v_floor, v_floor_until
    FROM floors f WHERE f.topic_id = p_topic;
  -- `written_at` grows with the position, so this walks only what it deletes, plus one.
  SELECT r.position INTO v_keep FROM records r
   WHERE r.topic_id = p_topic AND r.written_at >= v_expired ORDER BY r.position LIMIT 1;
  v_cut := coalesce(v_keep - 1, t.last_position);
  v_held := v_floor IS NOT NULL AND v_floor - 1 < v_cut;
  IF v_held THEN v_cut := v_floor - 1; END IF;
  SELECT r.position INTO v_limit FROM records r
   WHERE r.topic_id = p_topic AND r.position <= v_cut
   ORDER BY r.position OFFSET p_batch - 1 LIMIT 1;
  done := v_limit IS NULL OR v_limit = v_cut;
  v_cut := least(v_cut, v_limit);
  IF v_cut > coalesce(t.trimmed, t.base) THEN
    DELETE FROM records r WHERE r.topic_id = p_topic AND r.position <= v_cut;
    GET DIAGNOSTICS deleted = ROW_COUNT;
    IF deleted > 0 THEN t.trimmed := v_cut; END IF;
  END IF;
  DELETE FROM producers h WHERE h.topic_id = p_topic AND h.written_at < v_expired;
  SELECT r.written_at INTO v_oldest FROM records r WHERE r.topic_id = p_topic
   ORDER BY r.position LIMIT 1;
  IF v_oldest IS NULL AND v_floor IS NULL AND t.archive_pending IS NOT TRUE
     AND coalesce(t.written_at, '-infinity') < now() - v_retention - v_grace THEN
    -- The tombstone outlived its grace. The time-based base keeps later positions above it.
    DELETE FROM producers h WHERE h.topic_id = p_topic;
    DELETE FROM topics WHERE topic_id = p_topic;
    -- A busy chain keeps its row. A later pass of the idle chains takes it.
    IF pg_try_advisory_xact_lock(hashtext(current_schema()), (t.chain_id % 2147483647)::int) THEN
      DELETE FROM chains c WHERE c.chain_id = t.chain_id
         AND c.touched_at < now() - v_retention - v_grace
         AND NOT EXISTS (SELECT 1 FROM topics x WHERE x.chain_id = t.chain_id);
    END IF;
    done := true;
    RETURN;
  END IF;
  UPDATE topics SET trimmed = t.trimmed,
    trim_at = CASE
      WHEN NOT done THEN now()
      WHEN v_oldest IS NOT NULL AND v_floor IS NULL THEN v_oldest + v_retention
      -- The hold keeps the oldest record, so nothing moves before a hold ends.
      WHEN v_oldest IS NOT NULL AND v_held THEN v_floor_until
      WHEN v_oldest IS NOT NULL THEN least(v_oldest + v_retention, v_floor_until)
      WHEN v_floor IS NOT NULL THEN v_floor_until
      ELSE coalesce(t.written_at, now()) + v_retention + v_grace END
  WHERE topic_id = p_topic;
END $$;

-- As in migration 1, with at most 1,000 live holders per topic.
CREATE OR REPLACE FUNCTION hold_floor_v1(p_namespace text, p_kind smallint, p_owner text,
  p_run text, p_activity text, p_topic text, p_partition smallint, p_holder text,
  p_position bigint, p_lease_ms bigint) RETURNS void
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
  v_chain bigint;
  v_topic bigint;
BEGIN
  SELECT l.chain_id INTO v_chain
    FROM live_chain_v1(p_namespace, p_kind, p_owner, p_run, p_activity) l;
  LOOP
    v_topic := topic_id_v1(v_chain, p_topic, p_partition);
    -- The key share lock keeps the janitor from deleting the topic under the insert.
    PERFORM 1 FROM topics t WHERE t.topic_id = v_topic FOR KEY SHARE;
    EXIT WHEN FOUND;
  END LOOP;
  -- Every trim reads the holds, and each one pins records, so a topic takes at most 1,000 live
  -- holders, as `MAX_FLOOR_HOLDERS` says. A holder already on the topic renews.
  IF NOT EXISTS (SELECT 1 FROM floors f WHERE f.topic_id = v_topic AND f.holder = p_holder)
     AND (SELECT count(*) FROM floors f WHERE f.topic_id = v_topic AND f.until > now()) >= 1000
  THEN
    RAISE EXCEPTION 'topic % already has 1000 floor holders, so a positions-only reader can not '
                    'start on it', p_topic USING ERRCODE = 'P0001';
  END IF;
  -- Position 0 holds the whole topic. The lowest position a holder held wins.
  INSERT INTO floors AS f
  VALUES (v_topic, p_holder, p_position, now() + p_lease_ms * interval '1 millisecond')
  ON CONFLICT (topic_id, holder) DO UPDATE
    SET position = least(f.position, excluded.position), until = excluded.until;
END $$;

-- As in migration 2. The cap keeps the entries a driver took and works on.
CREATE OR REPLACE FUNCTION cap_log_v1(p_keep int) RETURNS bigint
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
      -- An entry a driver took and still works on stays, or a quiet stream that shares the
      -- partition with a busy one loses its only entry. One taken longer than a driver's
      -- default `claim_idle` counts as left behind.
      DELETE FROM notifications n WHERE n.partition = v_partition AND n.id <= v_oldest
         AND (n.taken_by IS NULL OR n.taken_at < now() - interval '1 minute');
      GET DIAGNOSTICS v_rows = ROW_COUNT;
      deleted := deleted + v_rows;
    END IF;
  END LOOP;
  RETURN deleted;
END $$;

-- As in migration 3. The hold and `archived` move forward only.
CREATE OR REPLACE FUNCTION advance_archive_v1(p_namespace text, p_kind smallint, p_owner text,
  p_run text, p_activity text, p_topic text, p_position bigint, p_lease_ms bigint) RETURNS void
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
  v_topic bigint;
  v_archived bigint;
  v_closed boolean;
BEGIN
  -- The key share lock keeps the janitor from deleting the topic under the insert.
  SELECT t.topic_id, t.archived, t.closed INTO v_topic, v_archived, v_closed FROM chains c
    JOIN topics t ON t.chain_id = c.chain_id AND t.topic = p_topic
   WHERE (c.namespace, c.owner_kind, c.owner_id, c.first_run_id, c.activity_id)
       = (p_namespace, p_kind, p_owner, p_run, p_activity)
     FOR KEY SHARE OF t;
  IF NOT FOUND THEN
    RETURN;
  END IF;
  -- Two drivers can archive from the same start. The slower one's advance must not move the
  -- hold back, nor hold a closed topic again after the archive released it.
  IF p_position < v_archived OR (p_position = v_archived AND v_closed) THEN
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
