-- Appends one batch of one Producer.
--
-- ARGV: format version, kind, Producer id, encoded run id, epoch key, sequence, encoded
-- withdrawn run, Supersession bytes, safety TTL ms, Tombstone ms, task key, completed task key
-- (`-` when the write gives no task facts), failed task ids, task Supersession bytes, the batch
-- digest, then one Record per argument.
--
-- Answers {'ok' or 'resend', first id, last id, '1' when flagged}.
local meta, log = KEYS[1], KEYS[2]
local version, kind, producer = tonumber(ARGV[1]), ARGV[2], ARGV[3]
local run, epoch, sequence = ARGV[4], ARGV[5], tonumber(ARGV[6])
local withdrawn_run, supersession = ARGV[7], ARGV[8]
local safety, tombstone = tonumber(ARGV[9]), tonumber(ARGV[10])
local task, completed, failed, task_supersession = ARGV[11], ARGV[12], ARGV[13], ARGV[14]
local digest = ARGV[15]
local records = #ARGV - 15
-- A Local Activity write with its run's task facts withdraws the run's orphaned Local Activity
-- Records: those of tasks that never completed.
local orphans = kind == '2' and completed ~= '-' and run ~= ''
local open_field, gone_field = 'oa|' .. run, 'ow|' .. run

if not sequence or sequence < 1 then
  return refuse('INVALID', 'the sequence must be 1 or more')
end
local now = now_ms()
local refused = check_live(meta, version, now)
if refused then
  return refused
end
local suffix = '|' .. kind .. '|' .. producer
-- The dedupe key goes before the close status, so a resend gets its Positions after a close.
local last = redis.call('HGET', meta, 'd' .. suffix)
if last then
  -- An entry written before the digest has none, and takes any resend.
  local last_epoch, last_sequence, first_id, last_id, last_digest =
    string.match(last, '^([^|]*)|(%d+)|([^|]*)|([^|]*)|?([^|]*)$')
  if last_epoch == epoch then
    last_sequence = tonumber(last_sequence)
    if sequence == last_sequence then
      if last_digest ~= '' and last_digest ~= digest then
        return refuse('SEQ_DIVERGENT', 'sequence ' .. sequence ..
          ' has other content than the batch the Store has')
      end
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
if kind == '2' and run ~= '' and contains(words(redis.call('HGET', meta, gone_field)), task) then
  return refuse('FENCED', 'a Superseded item withdrew the Workflow Task of the call')
end

refused = check_ids_fit(log, now, records + 2)
if refused then
  return refused
end

-- The first batch of a newer epoch withdraws the Records of the earlier ones. A client's (4)
-- newer epoch only fences, since its earlier Records are real messages.
local holds = redis.call('HGET', meta, 'h' .. suffix)
local flagged = kind ~= '4' and holds ~= false and holds < epoch
if withdraw_run(meta, withdrawn_run) then
  flagged = true
end
local withdrawn, open = {}, {}
if orphans then
  withdrawn, open = orphan_tasks(words(redis.call('HGET', meta, open_field)), task, completed,
    failed)
end

trim(meta, log, now)
local first_id, last_id = '', ''
-- The orphans go in an entry of their own, since the batch's first entry may carry its own
-- Superseded item.
local orphan_id = ''
if #withdrawn > 0 then
  orphan_id = add(log, {'s', task_supersession})
  last_id = orphan_id
end
if records == 0 and flagged then
  last_id = add(log, {'s', supersession})
  if first_id == '' then
    first_id = last_id
  end
end
for i = 16, #ARGV do
  local fields = {'r', ARGV[i]}
  if i == 16 and flagged then
    fields = {'s', supersession, 'r', ARGV[i]}
  end
  last_id = add(log, fields)
  if first_id == '' then
    first_id = last_id
  end
end

if first_id == '' then
  first_id = orphan_id
end

redis.call('HSET', meta, 'e' .. suffix, epoch)
redis.call('HSET', meta, 'd' .. suffix,
  epoch .. '|' .. sequence .. '|' .. first_id .. '|' .. last_id .. '|' .. digest)
if records > 0 then
  redis.call('HSET', meta, 'h' .. suffix, epoch)
elseif flagged then
  redis.call('HDEL', meta, 'h' .. suffix)
end
if run ~= '' and records > 0 then
  redis.call('HSET', meta, 'run|' .. run, '1')
end
if orphans then
  if records > 0 and not contains(open, task) then
    table.insert(open, task)
  end
  if #open > 0 then
    redis.call('HSET', meta, open_field, table.concat(open, ' '))
  else
    redis.call('HDEL', meta, open_field)
  end
  if #withdrawn > 0 then
    local all = words(redis.call('HGET', meta, gone_field))
    for _, key in ipairs(withdrawn) do
      table.insert(all, key)
    end
    redis.call('HSET', meta, gone_field, table.concat(all, ' '))
  end
end
if last_id ~= '' then
  redis.call('HSET', meta, 'newest', last_id)
end
refresh(meta, log, now, safety, tombstone)
local flag = ''
if flagged or #withdrawn > 0 then
  flag = '1'
end
return {'ok', first_id, last_id, flag}
