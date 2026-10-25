-- Closes the Stream with one close Record.
--
-- ARGV: format version, kind, Producer id, encoded run id, epoch key, Tombstone ms, close
-- Record, encoded Workflow id of the run chain that the close's ref names (empty for none).
--
-- Returns {'closed' or 'already', close id}.
local meta, log = KEYS[1], KEYS[2]
local version, kind, producer, run = tonumber(ARGV[1]), ARGV[2], ARGV[3], ARGV[4]
local epoch, tombstone, record = ARGV[5], tonumber(ARGV[6]), ARGV[7]
local chain = ARGV[8]

local now = now_ms()
local refused = check_live(meta, version, now)
if refused then
  return refused
end
refused = check_owner(meta, kind, chain)
if refused then
  return refused
end
if redis.call('HGET', meta, 'closed') then
  return {'already', redis.call('HGET', meta, 'close_id')}
end
if run ~= '' and redis.call('HGET', meta, 'wr|' .. run) then
  return refuse('FENCED', 'a Workflow retry withdrew the run of the Producer')
end
-- A Producer is fenced as its appends are, so a stale attempt can't close the Stream.
local newest = redis.call('HGET', meta, 'e|' .. kind .. '|' .. producer)
if newest and epoch < newest then
  return refuse('FENCED', 'the epoch is lower than the newest one of the Producer')
end

refused = check_ids_fit(meta, log, now, 1)
if refused then
  return refused
end

trim(meta, log, now)
local id = add(log, {'r', record})
redis.call('HSET', meta, 'closed', '1', 'close_id', id, 'newest', id)
-- Safety TTL 0, since a closed Stream keeps its Retention.
refresh(meta, log, now, 0, tombstone)
return {'closed', id}
