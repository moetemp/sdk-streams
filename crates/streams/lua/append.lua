-- Appends one batch of one Producer.
--
-- ARGV: format version, kind, Producer id, epoch key, sequence, Supersession bytes, safety TTL ms,
-- Tombstone ms, the batch digest, then one Record per argument.
--
-- Returns {'ok' or 'resend', first id, last id, '1' when flagged}.
local meta, log = KEYS[1], KEYS[2]
local version, kind, producer = tonumber(ARGV[1]), ARGV[2], ARGV[3]
local epoch, sequence_text = ARGV[4], ARGV[5]
local sequence = tonumber(sequence_text)
local supersession = ARGV[6]
local safety, tombstone = tonumber(ARGV[7]), tonumber(ARGV[8])
local digest = ARGV[9]
local records = #ARGV - 9

-- A Lua number past 2^53 - 1 loses digits, so the dedupe entry keeps the sequence as sent.
if not string.match(sequence_text, '^[1-9]%d*$') or sequence > 9007199254740991 then
  return refuse('INVALID', 'the sequence must be from 1 to 2^53 - 1')
end
local now = now_ms()
local refused = check_live(meta, version, now)
if refused then
  return refused
end
local suffix = '|' .. kind .. '|' .. producer
-- Check the dedupe key before the close status, so that a resend gets its entry ids after a close.
local last = redis.call('HGET', meta, 'd' .. suffix)
if last then
  local last_epoch, last_sequence, first_id, last_id, last_digest =
    string.match(last, '^([^|]*)|(%d+)|([^|]*)|([^|]*)|([^|]*)$')
  if last_epoch == epoch then
    last_sequence = tonumber(last_sequence)
    if sequence == last_sequence then
      if last_digest ~= digest then
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
local newest = redis.call('HGET', meta, 'e' .. suffix)
if newest and epoch < newest then
  return refuse('FENCED', 'the epoch is lower than the newest one of the Producer')
end

refused = check_ids_fit(meta, log, now, records + 1)
if refused then
  return refused
end

-- The first batch of a newer epoch withdraws the Records of the earlier ones. A client's (4)
-- newer epoch only fences, since its earlier Records are real messages.
local holds = redis.call('HGET', meta, 'h' .. suffix)
local flagged = kind ~= '4' and holds ~= false and holds < epoch

trim(meta, log, now)
local first_id, last_id = '', ''
if records == 0 and flagged then
  last_id = add(log, {'s', supersession})
  if first_id == '' then
    first_id = last_id
  end
end
for i = 10, #ARGV do
  local fields = {'r', ARGV[i]}
  if i == 10 and flagged then
    fields = {'s', supersession, 'r', ARGV[i]}
  end
  last_id = add(log, fields)
  if first_id == '' then
    first_id = last_id
  end
end

redis.call('HSET', meta, 'e' .. suffix, epoch)
redis.call('HSET', meta, 'd' .. suffix,
  epoch .. '|' .. sequence_text .. '|' .. first_id .. '|' .. last_id .. '|' .. digest)
if records > 0 then
  redis.call('HSET', meta, 'h' .. suffix, epoch)
elseif flagged then
  redis.call('HDEL', meta, 'h' .. suffix)
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
