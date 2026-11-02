CLAIM_LUA = """
local pending = KEYS[1]
local processing = KEYS[2]
local job_id = redis.call('LPOP', pending)
if not job_id then
  return false
end
redis.call('SADD', processing, job_id)
redis.call('SETEX', ARGV[1], tonumber(ARGV[2]), ARGV[3])
return job_id
"""

COMPLETE_LUA = """
local ok = redis.call('SET', KEYS[1], ARGV[1], 'NX')
if not ok then
  return 0
end
redis.call('SREM', KEYS[2], ARGV[2])
redis.call('DEL', ARGV[3])
return 1
"""
