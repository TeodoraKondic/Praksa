using Microsoft.AspNetCore.Mvc;

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

        if (string.IsNullOrWhiteSpace(request.Topic))
        {
            return BadRequest(new { message = "Topic cannot be empty." });
        }

        try
        {
            await _mqttService.SubscribeAsync(request.Topic, request.Qos);

            return Ok(new { message = "Subscribed successfully.", topic = request.Topic });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Subscribe failed: {ex.Message}");

            return BrokerUnavailable();
        }
    }

    [HttpPost("publish")]
    public async Task<IActionResult> Publish(PublishRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.Topic))
        {
            return BadRequest(new { message = "Topic cannot be empty." });
        }

        if (string.IsNullOrWhiteSpace(request.Payload))
        {
            return BadRequest(new { message = "Payload cannot be empty." });
        }

        if (request.Topic.Contains("+") || request.Topic.Contains("#"))
        {
            return BadRequest(new { message = "Topic cannot contain wildcards (+ or #) when publishing." });
        }


        if (request.Qos < 0 || request.Qos > 2)
        {
            return BadRequest(new { message = "QoS must be 0, 1, or 2." });
        }

        if (!_mqttService.IsConnected)
        {
            return BrokerUnavailable();
        }

        try
        {
            await _mqttService.PublishAsync(request.Topic, request.Payload, request.Qos);
            return Ok(new { message = "Message published" });
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Publish failed: {ex.Message}");

            return BrokerUnavailable();
        }

    }

    [HttpGet("messages")]
    public IActionResult GetMessages()
    {
        var messages = _mqttService.GetMessages();
        return Ok(new {messages});
    }

    private IActionResult BrokerUnavailable()
    {
        return StatusCode(StatusCodes.Status503ServiceUnavailable, new { message = "MQTT broker is not available." });
    }
}
