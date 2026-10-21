-- Appends one batch of one Producer.
--
-- ARGV: format version, kind, Producer id, encoded run id, epoch key, task key, sequence,
-- previous completed task key, failed task ids, encoded withdrawn run, Supersession bytes,
-- safety TTL ms, Tombstone ms, then one Record per argument.
--
-- Answers {'ok' or 'resend', first id, last id, '1' when flagged}.
local meta, log = KEYS[1], KEYS[2]
local version, kind, producer = tonumber(ARGV[1]), ARGV[2], ARGV[3]
local run, epoch, task = ARGV[4], ARGV[5], ARGV[6]
local sequence, completed, failed = tonumber(ARGV[7]), ARGV[8], ARGV[9]
local withdrawn_run, supersession = ARGV[10], ARGV[11]
local safety, tombstone = tonumber(ARGV[12]), tonumber(ARGV[13])
local records = #ARGV - 13
local workflow, local_activity = kind == '1', kind == '2'

local now = now_ms()
local refused = check_live(meta, version, now)
if refused then
  return refused
end
local suffix = '|' .. kind .. '|' .. producer
-- The dedupe key goes before the close status, so a resend gets its Positions after a close.
local last = redis.call('HGET', meta, 'd' .. suffix)
if last then
  local last_epoch, last_sequence, first_id, last_id =
    string.match(last, '^([^|]*)|(%d+)|([^|]*)|([^|]*)$')
  if last_epoch == epoch then
    last_sequence = tonumber(last_sequence)
    if sequence == last_sequence then
      return {'resend', first_id, last_id, ''}
    end
    if sequence < last_sequence then
      return refuse('SEQUENCE', 'sequence ' .. sequence .. ' is below the last one, ' ..
        last_sequence)
    end
  end
end
if redis.call('HGET', meta, 'closed') then
  return refuse('CLOSED', 'the Stream is closed')
end
if run ~= '' and redis.call('HGET', meta, 'wr|' .. run) then
  return refuse('FENCED', 'a Workflow retry withdrew the run of the Producer')
end
local newest = redis.call('HGET', meta, 'e' .. suffix)
if newest and epoch < newest then
  return refuse('FENCED', 'the epoch is lower than the newest one of the Producer')
end
if local_activity and contains(words(redis.call('HGET', meta, 'wt|' .. run)), task) then
  return refuse('FENCED', 'a Superseded item withdrew the Workflow Task of the call')
end

-- The batch goes in. First find what it withdraws, then write.
local flagged = false
local index_field, withdrawn_field = 'ti|' .. run, 'wt|' .. run
local index = words(redis.call('HGET', meta, index_field))
local withdrawn = {}
if workflow then
  withdrawn, index = withdraw_tasks(index, task, completed, failed)
  flagged = #withdrawn > 0
else
  local holds = redis.call('HGET', meta, 'h' .. suffix)
  flagged = holds ~= false and holds < epoch
end
if withdraw_run(meta, withdrawn_run) then
  flagged = true
end

trim(meta, log, now)
local first_id, last_id = '', ''
if records == 0 and flagged then
  first_id = add(log, {'s', supersession})
  last_id = first_id
end
for i = 14, #ARGV do
  local fields = {'r', ARGV[i]}
  if i == 14 and flagged then
    fields = {'s', supersession, 'r', ARGV[i]}
  end
  last_id = add(log, fields)
  if first_id == '' then
    first_id = last_id
  end
end

redis.call('HSET', meta, 'e' .. suffix, epoch)
redis.call('HSET', meta, 'd' .. suffix,
  epoch .. '|' .. sequence .. '|' .. first_id .. '|' .. last_id)
if not workflow then
  if records > 0 then
    redis.call('HSET', meta, 'h' .. suffix, epoch)
  elseif flagged then
    redis.call('HDEL', meta, 'h' .. suffix)
  end
end
if (workflow or local_activity) and records > 0 and not contains(index, task) then
  table.insert(index, task)
end
if workflow or local_activity then
  if #index > 0 then
    redis.call('HSET', meta, index_field, table.concat(index, ' '))
  else
    redis.call('HDEL', meta, index_field)
  end
end
if #withdrawn > 0 then
  local all = words(redis.call('HGET', meta, withdrawn_field))
  for _, key in ipairs(withdrawn) do
    table.insert(all, key)
  end
  redis.call('HSET', meta, withdrawn_field, table.concat(all, ' '))
end
if run ~= '' and records > 0 then
  redis.call('HSET', meta, 'run|' .. run, '1')
end
if last_id ~= '' then
  redis.call('HSET', meta, 'newest', last_id)
end
refresh(meta, log, now, safety, tombstone)
local flag = ''
if flagged then
  flag = '1'
end
return {'ok', first_id, last_id, flag}
