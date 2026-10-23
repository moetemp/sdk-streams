-- Creates the Stream's meta key, or finds it.
--
-- ARGV: format version, Retention ms, owner (`wf` or `client`), safety TTL ms, Tombstone ms,
-- encoded withdrawn run, Supersession bytes, auto close ms (empty for none), encoded Workflow id of
-- the owner's run chain (empty for none).
--
-- Answers {outcome, stored Retention ms, '1' when flagged, stored auto close ms}. The outcome
-- is `created`, `exists`, `conflict` for another Retention or auto close, `closed` or
-- `expired`.
local meta, log = KEYS[1], KEYS[2]
local version, retention, owner = tonumber(ARGV[1]), ARGV[2], ARGV[3]
local safety, tombstone = tonumber(ARGV[4]), tonumber(ARGV[5])
local withdrawn_run, supersession, period = ARGV[6], ARGV[7], ARGV[8]
local chain = ARGV[9]

local now = now_ms()
local stored = redis.call('HGET', meta, 'ret')
if not stored then
  redis.call('HSET', meta, 'v', version, 'ret', retention, 'own', owner)
  if owner == 'wf' and chain ~= '' then
    redis.call('HSET', meta, 'oc', chain)
  end
  if period ~= '' then
    -- The create counts as the first append, so a Stream that nobody writes still closes.
    redis.call('HSET', meta, 'ac', period, 'la', now)
  end
  withdraw_run(meta, withdrawn_run)
  refresh(meta, log, now, safety, tombstone)
  return {'created', retention, '', period}
end
local stored_period = redis.call('HGET', meta, 'ac') or ''
local refused = check_live(meta, version, now)
if refused then
  if string.find(refused['err'], '^STREAM_EXPIRED') then
    return {'expired', stored, '', stored_period}
  end
  return refused
end
if auto_close(meta, log, now, tombstone) then
  return {'closed', stored, '', stored_period}
end
local outcome = 'exists'
if stored ~= retention or stored_period ~= period then
  outcome = 'conflict'
end
-- An Activity may create the Workflow's default Stream first. The Workflow's create makes it
-- Workflow-owned, so it keeps the safety TTL while it is open.
if owner == 'wf' and redis.call('HGET', meta, 'own') ~= 'wf' then
  redis.call('HSET', meta, 'own', 'wf')
  if chain ~= '' then
    redis.call('HSET', meta, 'oc', chain)
  end
  refresh(meta, log, now, safety, tombstone)
end
local flag = ''
if withdraw_run(meta, withdrawn_run) then
  local id = add(log, {'s', supersession})
  redis.call('HSET', meta, 'newest', id)
  refresh(meta, log, now, safety, tombstone)
  flag = '1'
end
return {outcome, stored, flag, stored_period}
