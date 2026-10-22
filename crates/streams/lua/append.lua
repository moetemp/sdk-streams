-- Appends one batch of one Producer.
--
-- ARGV: format version, kind, Producer id, encoded run id, epoch key, sequence, encoded
-- withdrawn run, Supersession bytes, safety TTL ms, Tombstone ms, then one Record per argument.
--
-- Answers {'ok' or 'resend', first id, last id, '1' when flagged}.
local meta, log = KEYS[1], KEYS[2]
local version, kind, producer = tonumber(ARGV[1]), ARGV[2], ARGV[3]
local run, epoch, sequence = ARGV[4], ARGV[5], tonumber(ARGV[6])
local withdrawn_run, supersession = ARGV[7], ARGV[8]
local safety, tombstone = tonumber(ARGV[9]), tonumber(ARGV[10])
local records = #ARGV - 10

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
if auto_close(meta, log, now, tombstone) then
  return refuse('CLOSED', 'the Stream is closed')
end
if run ~= '' and redis.call('HGET', meta, 'wr|' .. run) then
  return refuse('FENCED', 'a Workflow retry withdrew the run of the Producer')
end
local newest = redis.call('HGET', meta, 'e' .. suffix)
if newest and epoch < newest then
  return refuse('FENCED', 'the epoch is lower than the newest one of the Producer')
end

refused = check_ids_fit(log, now, records + 1)
if refused then
  return refused
end

-- The first batch of a newer epoch withdraws the Records of the earlier ones.
local holds = redis.call('HGET', meta, 'h' .. suffix)
local flagged = holds ~= false and holds < epoch
if withdraw_run(meta, withdrawn_run) then
  flagged = true
end

trim(meta, log, now)
local first_id, last_id = '', ''
if records == 0 and flagged then
  first_id = add(log, {'s', supersession})
  last_id = first_id
end
for i = 11, #ARGV do
  local fields = {'r', ARGV[i]}
  if i == 11 and flagged then
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
if records > 0 then
  redis.call('HSET', meta, 'h' .. suffix, epoch)
elseif flagged then
  redis.call('HDEL', meta, 'h' .. suffix)
end
if run ~= '' and records > 0 then
  redis.call('HSET', meta, 'run|' .. run, '1')
end
-- Only Records hold off the auto close. An empty batch that withdraws adds no data.
if records > 0 and redis.call('HGET', meta, 'ac') then
  redis.call('HSET', meta, 'la', now)
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
