-- Closes the Stream with one close Record.
--
-- ARGV: format version, kind, Producer id, encoded run id, epoch key, task key, previous
-- completed task key, failed task ids, Supersession bytes, Tombstone ms, close Record.
--
-- Answers {'closed' or 'already', close id, '1' when flagged}.
local meta, log = KEYS[1], KEYS[2]
local version, kind, producer, run = tonumber(ARGV[1]), ARGV[2], ARGV[3], ARGV[4]
local epoch, task, completed, failed = ARGV[5], ARGV[6], ARGV[7], ARGV[8]
local supersession, tombstone, record = ARGV[9], tonumber(ARGV[10]), ARGV[11]

local now = now_ms()
local refused = check_live(meta, version, now)
if refused then
  return refused
end
if auto_close(meta, log, now, tombstone) then
  return {'already', redis.call('HGET', meta, 'close_id'), ''}
end
if run ~= '' and redis.call('HGET', meta, 'wr|' .. run) then
  return refuse('FENCED', 'a Workflow retry withdrew the run of the Producer')
end

refused = check_ids_fit(log, now, 1)
if refused then
  return refused
end

-- A Workflow close has no epoch fence. The run's next task may flush before the close of the
-- task before it goes out, and that close is still valid. Any other Producer is fenced as its
-- appends are, so a stale attempt can't close the Stream.
if kind ~= '1' then
  local newest = redis.call('HGET', meta, 'e|' .. kind .. '|' .. producer)
  if newest and epoch < newest then
    return refuse('FENCED', 'the epoch is lower than the newest one of the Producer')
  end
  if kind == '2' and (contains(words(redis.call('HGET', meta, 'wt|' .. run)), task)
      or withdrawn_by_facts(meta, run, task)) then
    return refuse('FENCED', 'a Superseded item withdrew the Workflow Task of the call')
  end
end
local flagged = false
local withdrawn = {}
local index_field, withdrawn_field = 'ti|' .. run, 'wt|' .. run
if kind == '1' then
  local index
  withdrawn, index = withdraw_tasks(words(redis.call('HGET', meta, index_field)), task,
    completed, failed)
  flagged = #withdrawn > 0
  keep_facts(meta, run, task, completed, failed)
  if #index > 0 then
    redis.call('HSET', meta, index_field, table.concat(index, ' '))
  else
    redis.call('HDEL', meta, index_field)
  end
end

trim(meta, log, now)
local fields = {'r', record}
if flagged then
  fields = {'s', supersession, 'r', record}
end
local id = add(log, fields)
if #withdrawn > 0 then
  local all = words(redis.call('HGET', meta, withdrawn_field))
  for _, key in ipairs(withdrawn) do
    table.insert(all, key)
  end
  redis.call('HSET', meta, withdrawn_field, table.concat(all, ' '))
end
redis.call('HSET', meta, 'closed', '1', 'close_id', id, 'newest', id)
-- Safety TTL 0, since a closed Stream keeps its Retention.
refresh(meta, log, now, 0, tombstone)
local flag = ''
if flagged then
  flag = '1'
end
return {'closed', id, flag}
