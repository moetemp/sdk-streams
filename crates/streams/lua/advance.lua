local function extend(key, ms, create)
  local ttl = redis.call('PTTL', key)
  if (ttl >= 0 or (create and ttl == -1)) and ttl < ms then
    redis.call('PEXPIRE', key, ms)
  end
end
local lease, grace = tonumber(ARGV[3]), tonumber(ARGV[4])
if redis.call('EXISTS', KEYS[3]) == 0 then
  -- The topic is gone, so its mark would only hold back the slot's other topics.
  if KEYS[4] then
    redis.call('ZREM', KEYS[4], KEYS[2])
  end
  return 0
end
-- Two drivers can archive from the same start. The slower one's advance must not move the hold
-- back, nor hold a closed topic again after the archive released it.
local archived = redis.call('HGET', KEYS[3], 'archived')
if archived and archived ~= '' then
  if id_below(ARGV[2], archived) then
    return 0
  end
  if ARGV[2] == archived and redis.call('HGET', KEYS[3], 'closed') then
    return 0
  end
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2] .. '|' .. (clock_ms() + lease))
redis.call('HSET', KEYS[3], 'archived', ARGV[2])
if KEYS[4] and redis.call('HGET', KEYS[3], 'last') == ARGV[2] then
  redis.call('ZREM', KEYS[4], KEYS[2])
end
extend(KEYS[1], lease, true)
extend(KEYS[2], lease, false)
extend(KEYS[3], lease + grace, false)
return 1
