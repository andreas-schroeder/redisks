local value = redis.call('GET', KEYS[1])
if value ~= nil then
    redis.call('DEL', KEYS[1])
    redis.call('SREM', KEYS[2], ARGV[1])
    return value
else
    return false
end