
local function now_ms()
  local time = redis.call('TIME')
  return tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
end
local function revive(log, meta)
  if redis.call('EXISTS', log) == 0 then
    local last = redis.call('HGET', meta, 'last')
    if last then
      redis.call('HSET', meta, 'trimmed', last)
    end
  end
end
local function sweep_sessions(meta, floor_ms)
  local cursor = redis.call('HGET', meta, 'hwscan') or '0'
  local found = redis.call('HSCAN', meta, cursor, 'MATCH', 'hw:*', 'COUNT', 50)
  local fields = found[2]
  for i = 1, #fields, 2 do
    local last_ms = string.match(fields[i + 1], '|(%d+)%-%d+|[^|]+$')
    if last_ms and tonumber(last_ms) < floor_ms then
      redis.call('HDEL', meta, fields[i])
    end
  end
  redis.call('HSET', meta, 'hwscan', found[1])
end
local function trim(log, meta, retention)
  local floor_ms = now_ms() - retention
  local floor = floor_ms .. '-0'
  local doomed = redis.call('XREVRANGE', log, '(' .. floor, '-', 'COUNT', 1)
  if #doomed > 0 then
    redis.call('XTRIM', log, 'MINID', floor)
    redis.call('HSET', meta, 'trimmed', doomed[1][1])
  end
  return floor_ms
end
local function keep(log, meta, added, last, retention, grace, floor_ms)
  redis.call('PEXPIRE', log, retention)
  redis.call('HINCRBY', meta, 'added', added)
  redis.call('HSET', meta, 'last', last)
  redis.call('PEXPIRE', meta, retention + grace)
  sweep_sessions(meta, floor_ms)
end
