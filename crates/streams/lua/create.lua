-- Creates the Stream's meta key, or finds it.
--
-- ARGV: format version, Retention ms, owner (`wf` or `client`), safety TTL ms, Tombstone ms,
-- encoded withdrawn run, Supersession bytes.
--
-- Answers {outcome, stored Retention ms, '1' when flagged}. The outcome is `created`,
-- `exists`, `conflict` for another Retention, `closed` or `expired`.
local meta, log = KEYS[1], KEYS[2]
local version, retention, owner = tonumber(ARGV[1]), ARGV[2], ARGV[3]
local safety, tombstone = tonumber(ARGV[4]), tonumber(ARGV[5])
local withdrawn_run, supersession = ARGV[6], ARGV[7]

local now = now_ms()
local stored = redis.call('HGET', meta, 'ret')
if not stored then
  redis.call('HSET', meta, 'v', version, 'ret', retention, 'own', owner)
  withdraw_run(meta, withdrawn_run)
  refresh(meta, log, now, safety, tombstone)
  return {'created', retention, ''}
end
local refused = check_live(meta, version, now)
if refused then
  if string.find(refused['err'], '^STREAM_EXPIRED') then
    return {'expired', stored, ''}
  end
  return refused
end
if redis.call('HGET', meta, 'closed') then
  return {'closed', stored, ''}
end
local outcome = 'exists'
if stored ~= retention then
  outcome = 'conflict'
end
-- An Activity may create the Workflow's default Stream first. The Workflow's create makes it
-- Workflow-owned, so it keeps the safety TTL while it is open.
if owner == 'wf' and redis.call('HGET', meta, 'own') ~= 'wf' then
  redis.call('HSET', meta, 'own', 'wf')
  refresh(meta, log, now, safety, tombstone)
end
local flag = ''
if withdraw_run(meta, withdrawn_run) then
  local id = add(log, {'s', supersession})
  redis.call('HSET', meta, 'newest', id)
  refresh(meta, log, now, safety, tombstone)
  flag = '1'
end
return {outcome, stored, flag}
