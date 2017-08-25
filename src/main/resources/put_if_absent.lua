if redis.call('SET', KEYS[1], ARGV[1], 'NX') then
  redis.call('SADD', KEYS[2], ARGV[2])
  return false
else
  return redis.call('GET', KEYS[1])
end