using StackExchange.Redis;

public class RedisService
{
    private readonly IDatabase _redis;

    public RedisService(string connectionString)
    {
        var connection = ConnectionMultiplexer.Connect(connectionString);
        _redis = connection.GetDatabase();
    }

    public async Task SaveMessageAsync(string topic, string payload)
    {
        var key = $"mqtt:{topic}:{DateTime.UtcNow.Ticks}";
        await _redis.StringSetAsync(key, payload, TimeSpan.FromHours(1));
    }
    public async Task IncrementTopicCountAsync(string topic)
    {
        var key = $"stats:{topic}:count";
        await _redis.StringIncrementAsync(key);
        await _redis.KeyExpireAsync(key, TimeSpan.FromHours(1));
    }
    public async Task<long> GetTopicCountAsync(string topic)
    {
        var key= $"stats:{topic}:count";
        var value= await _redis.StringGetAsync(key);

        if (!value.HasValue)
        {
            return 0;
        }
        return (long)value;
    }
}