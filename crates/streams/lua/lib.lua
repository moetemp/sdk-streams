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

-- Whether entry id a is below entry id b.
local function id_below(a, b)
  local a_ms, a_seq = string.match(a, '^(%d+)-(%d+)$')
  local b_ms, b_seq = string.match(b, '^(%d+)-(%d+)$')
  a_ms, b_ms = tonumber(a_ms), tonumber(b_ms)
  return a_ms < b_ms or (a_ms == b_ms and tonumber(a_seq) < tonumber(b_seq))
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
    -- A clock that steps back must not move the watermark back.
    local trimmed = redis.call('HGET', meta, 'trimmed')
    if not trimmed or id_below(trimmed, old[1][1]) then
      redis.call('HSET', meta, 'trimmed', old[1][1])
    end
    redis.call('XTRIM', log, 'MINID', min)
  end
end

-- Takes a retry run's previous run out of the Stream. Answers true when that run wrote here.
local function withdraw_run(meta, run)
  if run == '' or redis.call('HGET', meta, 'wr|' .. run) then
    return false
  end
  redis.call('HSET', meta, 'wr|' .. run, '1')
  return redis.call('HGET', meta, 'run|' .. run) ~= false
end

-- Refuses a write whose entry ids would not fit an offset, which packs the sequence part in 20
-- bits. Redis numbers the entries of one millisecond from the newest id on, so this happens
-- only when a clock steps back under a very busy Stream.
local function check_ids_fit(log, now, count)
  local newest = redis.call('XREVRANGE', log, '+', '-', 'COUNT', 1)
  if #newest == 0 then
    return nil
  end
  local ms, seq = string.match(newest[1][1], '^(%d+)-(%d+)$')
  if tonumber(ms) >= now and tonumber(seq) + count >= 1048576 then
    return refuse('STORAGE', 'the entry ids of this millisecond would not fit an offset')
  end
  return nil
end

local function add(log, fields)
  return redis.call('XADD', log, '*', unpack(fields))
end

-- The close Record of an auto close: an encoded StreamRecord with only kind = CLOSE (3). It has
-- no Producer, result or failure, so every script can write it with no argument.
local AUTO_CLOSE_RECORD = '\8\3'

-- Closes an open Stream whose auto close period passed since its last append, so the call that
-- finds it late acts as if the close came on time. Answers true when the Stream is closed after
-- the call, by this auto close or by an earlier close.
local function auto_close(meta, log, now, tombstone)
  if redis.call('HGET', meta, 'closed') then
    return true
  end
  local period = tonumber(redis.call('HGET', meta, 'ac'))
  if not period then
    return false
  end
  local last = tonumber(redis.call('HGET', meta, 'la')) or 0
  if now < last + period or check_ids_fit(log, now, 1) then
    return false
  end
  trim(meta, log, now)
  local id = add(log, {'r', AUTO_CLOSE_RECORD})
  redis.call('HSET', meta, 'closed', '1', 'close_id', id, 'newest', id)
  refresh(meta, log, now, 0, tombstone)
  return true
end
