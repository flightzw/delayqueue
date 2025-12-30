package delayqueue

// pending2ReadyScript atomically moves messages from pending to ready
// keys: pendingKey, readyKey
// argv: currentTime
// returns: ready message number
const pending2ReadyScript = `
local msgs = redis.call('ZRangeByScore', KEYS[1], '0', ARGV[1])  -- get ready msg
if (#msgs == 0) then return end
local args2 = {} -- keys to push into ready
for _,v in ipairs(msgs) do
	table.insert(args2, v) 
    if (#args2 == 4000) then
		redis.call('LPush', KEYS[2], unpack(args2))
		args2 = {}
	end
end
if (#args2 > 0) then 
	redis.call('LPush', KEYS[2], unpack(args2))
end
redis.call('ZRemRangeByScore', KEYS[1], '0', ARGV[1])  -- remove msgs from pending
return #msgs
`

// updateZSetScoreScript update score of a zset member if it exists
// KEYS[1]: zset
// ARGV[1]: score
// ARGV[2]: member
const updateZSetScoreScript = `
if redis.call('zrank', KEYS[1], ARGV[2]) ~= nil then
    return redis.call('zadd', KEYS[1], ARGV[1], ARGV[2])
else
    return 0
end
`

// ready2UnackScript atomically moves messages from ready to unack
// keys: readyKey/retryKey, unackKey
// argv: retryTime
const ready2UnackScript = `
local msg = redis.call('RPop', KEYS[1])
if (not msg) then return end
redis.call('ZAdd', KEYS[2], ARGV[1], msg)
return msg
`

// unack2RetryScript atomically moves messages from unack to retry which remaining retry count greater than 0,
// and moves messages from unack to garbage which  retry count is 0
// Because DelayQueue cannot determine garbage message before eval unack2RetryScript, so it cannot pass keys parameter to redisCli.Eval
// Therefore unack2RetryScript moves garbage message to garbageKey instead of deleting directly
// keys: unackKey, retryCountKey, retryKey, garbageKey
// argv: currentTime
// returns: {retryMsgs, failMsgs}
const unack2RetryScript = `
local unack2retry = function(msgs)
	local retryCounts = redis.call('HMGet', KEYS[2], unpack(msgs)) -- get retry count
	local retryMsgs = 0
	local failMsgs = 0
	for i,v in ipairs(retryCounts) do
		local k = msgs[i]
		if v ~= false and v ~= nil and v ~= '' and tonumber(v) > 0 then
			redis.call("HIncrBy", KEYS[2], k, -1) -- reduce retry count
			redis.call("LPush", KEYS[3], k) -- add to retry
			retryMsgs = retryMsgs + 1
		else
			redis.call("HDel", KEYS[2], k) -- del retry count
			redis.call("SAdd", KEYS[4], k) -- add to garbage
			failMsgs = failMsgs + 1
		end
	end
	return retryMsgs, failMsgs
end

local retryMsgs = 0
local failMsgs = 0
local msgs = redis.call('ZRangeByScore', KEYS[1], '0', ARGV[1])  -- get retry msg
if (#msgs == 0) then return end
if #msgs < 4000 then
	local d1, d2 = unack2retry(msgs)
	retryMsgs = retryMsgs + d1
	failMsgs = failMsgs + d2
else
	local buf = {}
	for _,v in ipairs(msgs) do
		table.insert(buf, v)
		if #buf == 4000 then
		    local d1, d2 = unack2retry(buf)
			retryMsgs = retryMsgs + d1
			failMsgs = failMsgs + d2
			buf = {}
		end
	end
	if (#buf > 0) then
		local d1, d2 = unack2retry(buf)
		retryMsgs = retryMsgs + d1
		failMsgs = failMsgs + d2
	end
end
redis.call('ZRemRangeByScore', KEYS[1], '0', ARGV[1])  -- remove msgs from unack
return {retryMsgs, failMsgs}
`
