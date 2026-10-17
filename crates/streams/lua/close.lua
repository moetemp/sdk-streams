
redis.call('HSET', KEYS[1], 'closed', '1')
local close = redis.call('HGET', KEYS[1], 'close')
if not close then
  close = ARGV[1]
  redis.call('HSET', KEYS[1], 'close', close)
end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
if KEYS[2] then
  redis.call('ZADD', KEYS[2], 0, ARGV[3])
end
return close
