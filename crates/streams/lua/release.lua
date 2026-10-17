local held_value = redis.call('HGET', KEYS[1], ARGV[1])
if KEYS[2] then
  redis.call('ZREM', KEYS[2], ARGV[3])
end
if not held_value then
  return 0
end
local id, until_ms = string.match(held_value, '^(%d+%-%d+)|(%d+)$')
local release_ms = clock_ms() + tonumber(ARGV[2])
if id and tonumber(until_ms) > release_ms then
  redis.call('HSET', KEYS[1], ARGV[1], id .. '|' .. release_ms)
end
return 1
