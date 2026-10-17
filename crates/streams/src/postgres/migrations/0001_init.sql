-- The stream store's tables and functions. The runner sets `search_path` to the store's schema
-- first, and every function keeps that path with `SET search_path FROM CURRENT`, so one file
-- serves any schema. A DBA who applies it by hand runs `SET search_path = <schema>, pg_temp`
-- first.

CREATE TABLE schema_migrations (
  version         int         PRIMARY KEY,
  name            text        NOT NULL,
  checksum        bytea       NOT NULL,
  compatible_from int         NOT NULL,
  applied_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE chains (
  chain_id     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  namespace    text     NOT NULL,
  owner_kind   smallint NOT NULL,          -- 1 Workflow, 2 Activity, 3 Workflow Activity
  owner_id     text     NOT NULL,          -- the Workflow id, or the Activity id for kind 2
  first_run_id text     NOT NULL,
  activity_id  text     NOT NULL DEFAULT '',
  closed       boolean  NOT NULL DEFAULT false,
  touched_at   timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT chains_key UNIQUE (namespace, owner_kind, owner_id, first_run_id, activity_id)
);
CREATE INDEX chains_touched_at ON chains (touched_at);

CREATE TABLE topics (
  topic_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  chain_id        bigint   NOT NULL REFERENCES chains ON DELETE CASCADE,
  topic           text     NOT NULL,
  partition       smallint NOT NULL,       -- notification log partition, 0..63
  base            bigint   NOT NULL,       -- positions start at base + 1
  last_position   bigint,                  -- newest ever, NULL before the first record
  trimmed         bigint,                  -- newest dropped, NULL before the first trim
  archived        bigint,                  -- newest archived, NULL before the first advance
  closed          boolean  NOT NULL DEFAULT false,
  close           bytea,                   -- the first close, a serialized StoredClose
  written         boolean  NOT NULL DEFAULT false,  -- in a Workflow Activity's topic set
  archive_pending boolean  NOT NULL DEFAULT false,  -- holds records the archive lacks
  written_at      timestamptz,
  trim_at         timestamptz,             -- when the janitor looks at the topic next
  CONSTRAINT topics_key UNIQUE (chain_id, topic)
) WITH (fillfactor = 70);
CREATE INDEX topics_trim_at ON topics (trim_at) WHERE trim_at IS NOT NULL;
CREATE INDEX topics_archive_pending ON topics (partition) WHERE archive_pending;

-- No foreign key, since its trigger would run once per record. Deletes go through `purge`.
CREATE TABLE records (
  topic_id   bigint      NOT NULL,
  position   bigint      NOT NULL,
  written_at timestamptz NOT NULL,
  record     bytea       NOT NULL,         -- a serialized StreamRecord, body codec-encoded
  PRIMARY KEY (topic_id, position)
) WITH (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_insert_scale_factor = 0.02);

CREATE TABLE producers (                   -- the newest batch per producer attempt
  topic_id       bigint NOT NULL,
  producer_id    text   NOT NULL,
  attempt        bigint NOT NULL,
  sequence       bigint NOT NULL,
  count          int    NOT NULL,
  first_position bigint NOT NULL,
  digest         bytea  NOT NULL,
  written_at     timestamptz NOT NULL,
  PRIMARY KEY (topic_id, producer_id, attempt)
) WITH (fillfactor = 70);

CREATE TABLE floors (                      -- positions-only readers and the archive
  topic_id bigint      NOT NULL REFERENCES topics ON DELETE CASCADE,
  holder   text        NOT NULL,
  position bigint      NOT NULL,           -- 0 holds the whole topic
  until    timestamptz NOT NULL,
  PRIMARY KEY (topic_id, holder)
);

CREATE TABLE purge (topic_id bigint PRIMARY KEY);  -- deleted topics whose rows remain

CREATE FUNCTION chain_id_v1(p_namespace text, p_kind smallint, p_owner text, p_run text,
  p_activity text) RETURNS bigint
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
  v_chain bigint;
BEGIN
  SELECT c.chain_id INTO v_chain FROM chains c
   WHERE (c.namespace, c.owner_kind, c.owner_id, c.first_run_id, c.activity_id)
       = (p_namespace, p_kind, p_owner, p_run, p_activity);
  IF NOT FOUND THEN
    INSERT INTO chains (namespace, owner_kind, owner_id, first_run_id, activity_id)
    VALUES (p_namespace, p_kind, p_owner, p_run, p_activity)
    ON CONFLICT ON CONSTRAINT chains_key DO NOTHING;
    SELECT c.chain_id INTO v_chain FROM chains c
     WHERE (c.namespace, c.owner_kind, c.owner_id, c.first_run_id, c.activity_id)
         = (p_namespace, p_kind, p_owner, p_run, p_activity);
  END IF;
  RETURN v_chain;
END $$;

-- The chain, with its advisory lock held shared until the commit, so neither a chain close nor
-- a delete of the chain runs until then. A delete that won the race makes the chain again. The
-- lock's first key is the schema's, so stores in one database never wait for each other.
CREATE FUNCTION live_chain_v1(p_namespace text, p_kind smallint, p_owner text, p_run text,
  p_activity text, OUT chain_id bigint, OUT closed boolean)
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
  LOOP
    chain_id := chain_id_v1(p_namespace, p_kind, p_owner, p_run, p_activity);
    PERFORM pg_advisory_xact_lock_shared(hashtext(current_schema()), (chain_id % 2147483647)::int);
    SELECT c.closed INTO closed FROM chains c WHERE c.chain_id = live_chain_v1.chain_id;
    EXIT WHEN FOUND;
  END LOOP;
END $$;

CREATE FUNCTION topic_id_v1(p_chain bigint, p_topic text, p_partition smallint)
RETURNS bigint
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
  v_topic bigint;
BEGIN
  SELECT t.topic_id INTO v_topic FROM topics t WHERE t.chain_id = p_chain AND t.topic = p_topic;
  IF NOT FOUND THEN
    -- The base is the clock in ms << 20, so a topic made again after its tombstone went
    -- starts above every position it had before.
    INSERT INTO topics (chain_id, topic, partition, base)
    VALUES (p_chain, p_topic, p_partition,
            (extract(epoch FROM clock_timestamp()) * 1000)::bigint << 20)
    ON CONFLICT ON CONSTRAINT topics_key DO NOTHING;
    SELECT t.topic_id INTO v_topic FROM topics t
     WHERE t.chain_id = p_chain AND t.topic = p_topic;
  END IF;
  RETURN v_topic;
END $$;

-- Status 0 appended, 1 repeat, 2 divergent, 3 stale, 4 closed. The log and archive arguments
-- take effect from later migrations, so the signature stays the same.
CREATE FUNCTION append_v1(
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
  IF p_notify_reads THEN
    PERFORM pg_notify(current_schema() || '_read_' || (v_topic.topic_id % 64),
                      v_topic.topic_id::text);
  END IF;
  status := 0;
  first_position := v_last + 1;
  last_position := v_last + v_count;
END $$;

CREATE FUNCTION close_chain_v1(p_namespace text, p_kind smallint, p_owner text, p_run text,
  p_activity text) RETURNS void
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
  v_chain bigint;
BEGIN
  LOOP
    v_chain := chain_id_v1(p_namespace, p_kind, p_owner, p_run, p_activity);
    -- Waits for the appends in flight, and every later append sees the flag.
    PERFORM pg_advisory_xact_lock(hashtext(current_schema()), (v_chain % 2147483647)::int);
    UPDATE chains SET closed = true, touched_at = now() WHERE chain_id = v_chain;
    EXIT WHEN FOUND;
  END LOOP;
END $$;

-- Answers the close the topic keeps, which is the first one. `p_archive` takes effect from a
-- later migration.
CREATE FUNCTION close_topic_v1(p_namespace text, p_kind smallint, p_owner text, p_run text,
  p_activity text, p_topic text, p_partition smallint, p_close bytea, p_archive boolean)
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
    UPDATE topics t SET closed = true, close = coalesce(t.close, p_close)
     WHERE t.topic_id = v_topic
    RETURNING t.close INTO v_close;
    EXIT WHEN FOUND;
  END LOOP;
  RETURN v_close;
END $$;

CREATE FUNCTION hold_floor_v1(p_namespace text, p_kind smallint, p_owner text, p_run text,
  p_activity text, p_topic text, p_partition smallint, p_holder text, p_position bigint,
  p_lease_ms bigint) RETURNS void
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
  -- Position 0 holds the whole topic. The lowest position a holder held wins.
  INSERT INTO floors AS f
  VALUES (v_topic, p_holder, p_position, now() + p_lease_ms * interval '1 millisecond')
  ON CONFLICT (topic_id, holder) DO UPDATE
    SET position = least(f.position, excluded.position), until = excluded.until;
END $$;

CREATE FUNCTION release_floor_v1(p_namespace text, p_kind smallint, p_owner text, p_run text,
  p_activity text, p_topic text, p_holder text, p_grace_ms bigint) RETURNS void
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
BEGIN
  UPDATE floors f SET until = least(f.until, now() + p_grace_ms * interval '1 millisecond')
    FROM chains c JOIN topics t ON t.chain_id = c.chain_id AND t.topic = p_topic
   WHERE (c.namespace, c.owner_kind, c.owner_id, c.first_run_id, c.activity_id)
       = (p_namespace, p_kind, p_owner, p_run, p_activity)
     AND f.topic_id = t.topic_id AND f.holder = p_holder;
END $$;

-- Deletes an owner's chains. Kind 1 (Workflow) also takes its Workflow Activities, kind 3.
-- Answers chains plus topics. The records go through `purge`, in batches after the commit.
CREATE FUNCTION delete_owner_v1(p_namespace text, p_kind smallint, p_owner text)
RETURNS bigint
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
  v record;
  v_topics bigint;
  deleted bigint := 0;
BEGIN
  FOR v IN SELECT c.chain_id FROM chains c
            WHERE c.namespace = p_namespace AND c.owner_id = p_owner
              AND c.owner_kind = ANY (CASE p_kind WHEN 1 THEN '{1,3}'::smallint[]
                                                  ELSE ARRAY[p_kind] END)
            ORDER BY c.chain_id
  LOOP
    -- Waits for the chain's appends in flight.
    PERFORM pg_advisory_xact_lock(hashtext(current_schema()), (v.chain_id % 2147483647)::int);
    INSERT INTO purge SELECT t.topic_id FROM topics t WHERE t.chain_id = v.chain_id
    ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS v_topics = ROW_COUNT;
    DELETE FROM chains c WHERE c.chain_id = v.chain_id;
    deleted := deleted + 1 + v_topics;
  END LOOP;
  RETURN deleted;
END $$;

-- Deletes at most `p_batch` rows of one purged topic. `done` is true when no topic is left that
-- another session isn't purging now.
CREATE FUNCTION purge_v1(p_batch int, OUT deleted bigint, OUT done boolean)
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
  v_topic bigint;
BEGIN
  deleted := 0;
  SELECT p.topic_id INTO v_topic FROM purge p ORDER BY p.topic_id LIMIT 1
    FOR UPDATE SKIP LOCKED;
  IF NOT FOUND THEN
    done := true;
    RETURN;
  END IF;
  DELETE FROM records r WHERE r.topic_id = v_topic AND r.position <= (
    SELECT max(x.position) FROM (
      SELECT k.position FROM records k WHERE k.topic_id = v_topic
       ORDER BY k.position LIMIT p_batch) x);
  GET DIAGNOSTICS deleted = ROW_COUNT;
  IF deleted < p_batch THEN
    DELETE FROM producers h WHERE h.topic_id = v_topic;
    DELETE FROM purge p WHERE p.topic_id = v_topic;
  END IF;
  done := false;
END $$;

-- One janitor step on one topic. `done` is false when a batch limit or a busy row cut it short.
CREATE FUNCTION trim_topic_v1(p_topic bigint, p_retention_ms bigint, p_grace_ms bigint,
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
  IF v_floor IS NOT NULL THEN v_cut := least(v_cut, v_floor - 1); END IF;
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
      WHEN v_oldest IS NOT NULL THEN least(v_oldest + v_retention, v_floor_until)
      WHEN v_floor IS NOT NULL THEN v_floor_until
      ELSE coalesce(t.written_at, now()) + v_retention + v_grace END
  WHERE topic_id = p_topic;
END $$;

-- Deletes chains with no topics that nothing touched for the retention plus the grace. A chain
-- that an append holds now is skipped.
CREATE FUNCTION drop_idle_chains_v1(p_retention_ms bigint, p_grace_ms bigint, p_max int)
RETURNS bigint
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
  v record;
  deleted bigint := 0;
BEGIN
  FOR v IN SELECT c.chain_id FROM chains c
            WHERE c.touched_at < now() - (p_retention_ms + p_grace_ms) * interval '1 millisecond'
              AND NOT EXISTS (SELECT 1 FROM topics t WHERE t.chain_id = c.chain_id)
            ORDER BY c.touched_at LIMIT p_max
  LOOP
    IF pg_try_advisory_xact_lock(hashtext(current_schema()), (v.chain_id % 2147483647)::int) THEN
      DELETE FROM chains c WHERE c.chain_id = v.chain_id
         AND NOT EXISTS (SELECT 1 FROM topics t WHERE t.chain_id = v.chain_id);
      deleted := deleted + 1;
    END IF;
  END LOOP;
  RETURN deleted;
END $$;
