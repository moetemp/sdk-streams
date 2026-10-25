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

-- Returns nil when the Stream accepts writes, or else the refusal. An open Stream whose `exp`
-- passed is a Tombstone, and only a read still uses it.
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

-- Returns nil when the Producer kind may close the Stream, or else the refusal. Only the Workflow
-- Producer (1) closes a Workflow-owned Stream, with a ref of the run chain in `oc`. A Local
-- Activity (2) can't, since it writes before its task commits. A client (4) closes the others.
local function check_owner(meta, kind, chain)
  if redis.call('HGET', meta, 'own') == 'wf' then
    if kind ~= '1' then
      return refuse('NOT_OWNER', 'only the close Activity of the owner run chain closes a ' ..
        'Workflow-owned Stream')
    end
    local owner_chain = redis.call('HGET', meta, 'oc')
    if owner_chain and chain ~= owner_chain then
      return refuse('NOT_OWNER', 'the ref names another run chain than the owner')
    end
  elseif kind ~= '4' then
    return refuse('NOT_OWNER', 'only a client closes a Stream that a client created')
  end
  return nil
end

-- Sets the expiry after this write. It's the safety TTL for an open Workflow-owned Stream, or the
-- Retention otherwise. The meta key expires 30 days later, as the Tombstone.
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

local function words(text)
  local list = {}
  if text then
    for word in string.gmatch(text, '%S+') do
      table.insert(list, word)
    end
  end
  return list
end

local function contains(list, value)
  for _, item in ipairs(list) do
    if item == value then
      return true
    end
  end
  return false
end

local function started(task_key)
  return tonumber(string.sub(task_key, 1, 16), 16)
end

-- Whether a task key at c's started id is a lower attempt than c. Lang sends c with attempt 0 when
-- it knows only c's started id. Then the started time decides, since the attempts of one task start
-- in time order.
local function below_completed(key, c)
  if string.sub(c, 18, 25) == '00000000' then
    return string.sub(key, 27) < string.sub(c, 27)
  end
  return key < c
end

-- The orphan rule over the open Local Activity tasks of a run, for a write in task t with the run's
-- last completed task c and its failed tasks. A task strictly between c and t, a failed one, or a
-- lower attempt of t or c never completed, so the rule withdraws it. The list drops the other tasks
-- at or before c, since they completed. Returns the withdrawn and the open tasks.
local function orphan_tasks(open, t, c, failed)
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
  for _, key in ipairs(open) do
    local s = started(key)
    if failed_set[s] or (cs < s and s < ts) or (s == ts and key < t)
        or (c ~= '' and s == cs and below_completed(key, c)) then
      table.insert(withdrawn, key)
    elseif c == '' or key > c then
      table.insert(kept, key)
    end
  end
  return withdrawn, kept
end

-- Withdraws a retry run's previous run from the Stream. Returns true when that run wrote here.
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
