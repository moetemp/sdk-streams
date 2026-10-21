-- The prelude of every script. KEYS[1] is the Stream's meta hash and KEYS[2] its log. Each
-- script checks everything that can refuse the call before its first write, since Redis doesn't
-- roll a script back.

local function now_ms()
  local time = redis.call('TIME')
  return tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)
end

local function refuse(code, message)
  return redis.error_reply('STREAM_' .. code .. ' ' .. message)
end

-- Nil when the Stream takes writes, or the refusal. An open Stream whose `exp` passed is a
-- Tombstone, which only a read still uses.
local function check_live(meta, version, now)
  local v = redis.call('HGET', meta, 'v')
  if not v then
    return refuse('NOT_FOUND', 'nobody created the Stream')
  end
  if tonumber(v) > version then
    return refuse('FORMAT_TOO_NEW', 'format ' .. v .. ' is newer than ' .. version)
  end
  local exp = tonumber(redis.call('HGET', meta, 'exp'))
  if exp and exp <= now then
    return refuse('EXPIRED', 'the Stream expired')
  end
  return nil
end

-- Sets the time the Stream stays after this write: the safety TTL for an open Workflow-owned
-- Stream, the Retention otherwise. The meta stays 30 days longer as the Tombstone.
local function refresh(meta, log, now, safety, tombstone)
  local ttl = tonumber(redis.call('HGET', meta, 'ret'))
  if redis.call('HGET', meta, 'own') == 'wf' and not redis.call('HGET', meta, 'closed') then
    ttl = safety
  end
  local exp = now + ttl
  redis.call('HSET', meta, 'exp', exp)
  redis.call('PEXPIREAT', meta, exp + tombstone)
  if redis.call('EXISTS', log) == 1 then
    redis.call('PEXPIREAT', log, exp)
  end
end

-- Drops the Records older than the Retention and keeps the newest dropped id, so a read can
-- tell expired Records from an empty Stream.
local function trim(meta, log, now)
  local min = now - tonumber(redis.call('HGET', meta, 'ret'))
  if min <= 0 then
    return
  end
  local old = redis.call('XREVRANGE', log, '(' .. min .. '-0', '-', 'COUNT', 1)
  if #old > 0 then
    redis.call('HSET', meta, 'trimmed', old[1][1])
    redis.call('XTRIM', log, 'MINID', min)
  end
end

local function words(text)
  local list = {}
  if text then
    for word in string.gmatch(text, '%S+') do
      table.insert(list, word)
    end
  end
  return list
end

local function started(task_key)
  return tonumber(string.sub(task_key, 1, 16), 16)
end

-- The Workflow withdrawal rule over the run's task index, for task t with the previous
-- completed task c. It answers the task keys it withdraws and the ones that stay. A task at or
-- before c that stays completed, so the index drops it.
local function withdraw_tasks(index, t, c, failed)
  local failed_set = {}
  for _, id in ipairs(words(failed)) do
    failed_set[tonumber(id)] = true
  end
  local ts = started(t)
  local cs = 0
  if c ~= '' then
    cs = started(c)
  end
  local withdrawn, kept = {}, {}
  for _, key in ipairs(index) do
    local s = started(key)
    if failed_set[s] or (cs < s and s < ts) or (s == ts and key < t)
        or (c ~= '' and s == cs and key < c) then
      table.insert(withdrawn, key)
    elseif c == '' or key > c then
      table.insert(kept, key)
    end
  end
  return withdrawn, kept
end

local function contains(list, value)
  for _, item in ipairs(list) do
    if item == value then
      return true
    end
  end
  return false
end

-- Takes a retry run's previous run out of the Stream. Answers true when that run wrote here.
local function withdraw_run(meta, run)
  if run == '' or redis.call('HGET', meta, 'wr|' .. run) then
    return false
  end
  redis.call('HSET', meta, 'wr|' .. run, '1')
  return redis.call('HGET', meta, 'run|' .. run) ~= false
end

local function add(log, fields)
  return redis.call('XADD', log, '*', unpack(fields))
end
