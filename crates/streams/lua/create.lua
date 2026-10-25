-- Creates the Stream's meta key, or finds it.
--
-- ARGV: format version, Retention ms, owner (`wf` or `client`), safety TTL ms, Tombstone ms,
-- encoded Workflow id of the owner's run chain (empty for none).
--
-- Returns {outcome, stored Retention ms}. The outcome is `created`, `exists`, `conflict` for
-- another Retention, `closed` or `expired`.
local meta, log = KEYS[1], KEYS[2]
local version, retention, owner = tonumber(ARGV[1]), ARGV[2], ARGV[3]
local safety, tombstone = tonumber(ARGV[4]), tonumber(ARGV[5])
local chain = ARGV[6]

local now = now_ms()
local stored = redis.call('HGET', meta, 'ret')
if not stored then
  redis.call('HSET', meta, 'v', version, 'ret', retention, 'own', owner)
  if owner == 'wf' and chain ~= '' then
    redis.call('HSET', meta, 'oc', chain)
  end
  refresh(meta, log, now, safety, tombstone)
  return {'created', retention}
end
local refused = check_live(meta, version, now)
if refused then
  if string.find(refused['err'], '^STREAM_EXPIRED') then
    return {'expired', stored}
  end
  return refused
end
if redis.call('HGET', meta, 'closed') then
  return {'closed', stored}
end
local outcome = 'exists'
if stored ~= retention then
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
return {outcome, stored}
