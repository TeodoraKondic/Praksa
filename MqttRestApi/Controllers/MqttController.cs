using System.Data.Common;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using MySqlConnector;
using StackExchange.Redis;

[ApiController]
[Route("api/[controller]")]

public class MqttController : ControllerBase
{
    private readonly IMqttService _mqttService;
    private readonly RedisService _redisService;
    private readonly AppDbContext _db;
    public MqttController(IMqttService mqttService, RedisService redisService, AppDbContext db)
    {
        _mqttService = mqttService;
        _redisService = redisService;
        _db=db;
    }

    [HttpPost("subscribe")]
    public async Task<IActionResult> Subscribe(SubscribeRequest request)
    {
        if (!_mqttService.IsConnected)
        {
            return BrokerUnavailable();
        }

        try
        {
            await _mqttService.SubscribeAsync(request.Topic, request.Qos);
            //return Ok(new { message = "Subscribed successfully", topic = request.Topic });
        }
        catch (Exception ex) when (ex is DbException || ex is DbUpdateException)
        {
            Console.WriteLine($"Subscribe error: {ex.Message}");
            return DatabaseUnavailable();
        }
        /*catch (MySqlException ex)
        {
            Console.WriteLine($"Database error: {ex.Message}");
            return DatabaseUnavailable();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Mqtt subscription failed: {ex.Message}");
            return BrokerUnavailable();
        }*/
        return Ok(new { message = "Subscribed successfully", topic = request.Topic });

    }

    [HttpPost("publish")]
    public async Task<IActionResult> Publish(PublishRequest request)
    {
        if (request.Topic.Contains("+") || request.Topic.Contains("#"))
        {
            ModelState.AddModelError(nameof(request.Topic), "Topic cannot contain wildcards (+ or #) when publishing.");
            return ValidationProblem(ModelState);
        }
        if (!_mqttService.IsConnected)
        {
            return BrokerUnavailable();
        }

        await _mqttService.PublishAsync(request.Topic, request.Payload, request.Qos);
        return Ok(new { message = "Message published" });

    }

    [HttpGet("messages")]
    public async Task<IActionResult> GetMessages()
    {
        try
        {
            var messages = await _mqttService.GetMessagesAsync();
            return Ok(new { messages });
        }
        catch (Exception ex) when (ex is DbException || ex is DbUpdateException)
        {
            Console.WriteLine($"GetMessages failed: {ex.Message}");
            return DatabaseUnavailable();
        }
    }
    [HttpGet("stats/{*topic}")]
    public async Task<IActionResult> GetTopicStats(string topic)
    {
        try
        {
            var count =await _redisService.GetTopicCountAsync(topic);
            return Ok(new{topic, messageCount=count, timeWindow="1 hour"});
        }
        catch (RedisException ex)
        {
            Console.WriteLine($"GetTopicStats failed: {ex.Message}");
            return RedisUnavailable();
        }
    }

    [HttpGet("stats")]
    public async  Task<IActionResult> GetAllStats()
    {
        try
        {
            var topics = await _db.Subscriptions.AsNoTracking().Select(s => s.Topic).ToListAsync();

            var stats = new List<object>();

            foreach (var topic in topics)
            {
                var count = await _redisService.GetTopicCountAsync(topic);
                stats.Add(new { topic, messageCount = count, timeWindow = "1 hour" });
            }

            return Ok(stats);
        }
        catch(Exception ex) when (ex is DbException || ex is DbUpdateException)
        {
            Console.WriteLine($"GetAllStats failed: {ex.Message}");
            return DatabaseUnavailable();
        }
        catch(RedisException ex)
        {
            Console.WriteLine($"GetAllStats failed: {ex.Message}");
            return RedisUnavailable();
        }
    }

    private IActionResult BrokerUnavailable()
    {
        return Problem(detail: "MQTT broker is not available.", statusCode: StatusCodes.Status503ServiceUnavailable);
    }
    private IActionResult DatabaseUnavailable()
    {
        return Problem(detail: "Database is not available.", statusCode: StatusCodes.Status503ServiceUnavailable);
    }
    private IActionResult RedisUnavailable()
    {
        return Problem(detail: "Redis is not available.", statusCode: StatusCodes.Status503ServiceUnavailable);
    }
}
