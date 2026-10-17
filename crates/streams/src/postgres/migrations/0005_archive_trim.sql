-- A move or an end of a hold makes the topic due again, since a topic that a hold held waits
-- for the hold's end otherwise.

-- As in migration 4. The advance also makes the topic due at once, since the hold no longer
-- keeps what it archived.
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
         archive_pending = t.archive_pending AND t.last_position IS DISTINCT FROM p_position,
         trim_at = least(t.trim_at, now())
   WHERE t.topic_id = v_topic;
END $$;

-- As in migration 3. The topic is due when the released hold ends.
CREATE OR REPLACE FUNCTION release_floor_v1(p_namespace text, p_kind smallint, p_owner text,
  p_run text, p_activity text, p_topic text, p_holder text, p_grace_ms bigint) RETURNS void
LANGUAGE plpgsql SET search_path FROM CURRENT AS $$
DECLARE
  v_until timestamptz := now() + p_grace_ms * interval '1 millisecond';
BEGIN
  UPDATE floors f SET until = least(f.until, v_until)
    FROM chains c JOIN topics t ON t.chain_id = c.chain_id AND t.topic = p_topic
   WHERE (c.namespace, c.owner_kind, c.owner_id, c.first_run_id, c.activity_id)
       = (p_namespace, p_kind, p_owner, p_run, p_activity)
     AND f.topic_id = t.topic_id AND f.holder = p_holder;
  UPDATE topics t SET trim_at = least(t.trim_at, v_until),
         archive_pending = t.archive_pending AND p_holder <> 'archive'
    FROM chains c
   WHERE (c.namespace, c.owner_kind, c.owner_id, c.first_run_id, c.activity_id)
       = (p_namespace, p_kind, p_owner, p_run, p_activity)
     AND t.chain_id = c.chain_id AND t.topic = p_topic;
END $$;
