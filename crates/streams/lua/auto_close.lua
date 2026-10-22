-- Applies the Stream's auto close, for a read or a describe that found it due. A read script
-- writes nothing, so it runs on a replica, and this step goes to the primary.
--
-- ARGV: format version, Tombstone ms.
--
-- Answers {'closed' or 'open', close id}.
local meta, log = KEYS[1], KEYS[2]
local version, tombstone = tonumber(ARGV[1]), tonumber(ARGV[2])

local now = now_ms()
local refused = check_live(meta, version, now)
if refused then
  return refused
end
if auto_close(meta, log, now, tombstone) then
  return {'closed', redis.call('HGET', meta, 'close_id')}
end
return {'open', ''}
