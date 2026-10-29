using System.Data.Common;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using MySqlConnector;

[ApiController]
[Route("api/[controller]")]

public class MqttController : ControllerBase
{
    private readonly IMqttService _mqttService;

    public MqttController(IMqttService mqttService)
    {
        _mqttService = mqttService;
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
            return Ok(new { message = "Subscribed successfully", topic = request.Topic });
        }
        catch (DbUpdateException ex)
        {
            Console.WriteLine($"Database error: {ex.Message}");
            return DatabaseUnavailable();
        }
        catch (MySqlException ex)
        {
            Console.WriteLine($"Database error: {ex.Message}");
            return DatabaseUnavailable();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Mqtt subscription failed: {ex.Message}");
            return BrokerUnavailable();
        }

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

    private IActionResult BrokerUnavailable()
    {
        return Problem(detail: "MQTT broker is not available.", statusCode: StatusCodes.Status503ServiceUnavailable);
    }
    private IActionResult DatabaseUnavailable()
    {
        return Problem(detail: "Database is not available.", statusCode: StatusCodes.Status503ServiceUnavailable);
    }
}
