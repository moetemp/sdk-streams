local function clock_ms()
  local time = redis.call('TIME')
  return tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
end
local function id_below(a, b)
  local am, as = string.match(a, '^(%d+)%-(%d+)$')
  local bm, bs = string.match(b, '^(%d+)%-(%d+)$')
  am, as, bm, bs = tonumber(am), tonumber(as), tonumber(bm), tonumber(bs)
  return am < bm or (am == bm and as < bs)
end
local function held(floors, floor, retention)
  local now = clock_ms()
  local life = retention
  local entries = redis.call('HGETALL', floors)
  for i = 1, #entries, 2 do
    local id, until_ms = string.match(entries[i + 1], '^(%d+%-%d+)|(%d+)$')
    until_ms = tonumber(until_ms)
    if not until_ms or until_ms <= now then
      redis.call('HDEL', floors, entries[i])
    else
      if id_below(id, floor) then
        floor = id
      end
      life = math.max(life, until_ms - now)
    end
  end
  return floor, life
end
