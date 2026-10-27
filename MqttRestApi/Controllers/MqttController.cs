using Microsoft.AspNetCore.Mvc;

[ApiController]
[Route("api/[controller]")]

public class MqttController : ControllerBase
{
    private readonly MqttService _mqttService;

    public MqttController(MqttService mqttService)
    {
        _mqttService = mqttService;
    }

    [HttpPost("subscribe")]
    public async Task<IActionResult> Subscribe(SubscribeRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.Topic))
        {
            return BadRequest(new { message = "Topic cannot be empty." });
        }
            

        await _mqttService.SubscribeAsync(request.Topic);
        return Ok(new { message = $"Subscribed to {request.Topic}" });
    }

    [HttpPost("publish")]
    public async Task<IActionResult> Publish(PublishRequest request)
    {
        await _mqttService.PublishAsync(request.Topic, request.Payload, request.Qos);
        return Ok(new { message = "Message published" });
    }

    [HttpGet("messages")]
    public IActionResult GetMessages()
    {
        var messages = _mqttService.GetMessages();
        return Ok(messages);
    }
}
