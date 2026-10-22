-- Reads the Stream's state and up to ARGV[2] log entries from the id ARGV[1], inclusive, in one
-- step, so the entries and the state agree.
--
-- Answers {state, server time, entries}. The state is the meta fields v, closed, close_id,
-- newest, trimmed, ret, exp, ac and la, with false for a missing one.
local meta, log = KEYS[1], KEYS[2]
local state = redis.call('HMGET', meta, 'v', 'closed', 'close_id', 'newest', 'trimmed', 'ret',
  'exp', 'ac', 'la')
local entries = {}
if tonumber(ARGV[2]) > 0 then
  entries = redis.call('XRANGE', log, ARGV[1], '+', 'COUNT', ARGV[2])
end
return {state, redis.call('TIME'), entries}
