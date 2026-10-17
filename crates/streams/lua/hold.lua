local now = clock_ms()
local lease, grace = tonumber(ARGV[3]), tonumber(ARGV[4])
local id = ARGV[2]
local held_value = redis.call('HGET', KEYS[1], ARGV[1])
if not held_value and redis.call('HLEN', KEYS[1]) >= tonumber(ARGV[5]) then
  -- Lapsed holds count only until a scan drops them.
  local entries = redis.call('HGETALL', KEYS[1])
  for i = 1, #entries, 2 do
    local until_ms = tonumber(string.match(entries[i + 1], '|(%d+)$'))
    if not until_ms or until_ms <= now then
      redis.call('HDEL', KEYS[1], entries[i])
    end
  end
  if redis.call('HLEN', KEYS[1]) >= tonumber(ARGV[5]) then
    return redis.error_reply('STREAMS_HOLDERS topic already has ' .. ARGV[5] ..
      ' floor holders, so a positions-only reader can not start on it')
  end
end
if held_value then
  local old = string.match(held_value, '^(%d+%-%d+)|')
  if old and id_below(old, id) then
    id = old
  end
end
redis.call('HSET', KEYS[1], ARGV[1], id .. '|' .. (now + lease))
local function extend(key, ms, create)
  local ttl = redis.call('PTTL', key)
  if (ttl >= 0 or (create and ttl == -1)) and ttl < ms then
    redis.call('PEXPIRE', key, ms)
  end
end
extend(KEYS[1], lease, true)
extend(KEYS[2], lease, false)
extend(KEYS[3], lease + grace, false)
return 1
