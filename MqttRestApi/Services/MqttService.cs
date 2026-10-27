using System.Text;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;

public class MqttService
{
    private readonly IMqttClient _client;
    private readonly MqttFactory _factory;
    private readonly List<string> _messages = new List<string>();
    private readonly object _lock = new object();

    public MqttService()
    {
        _factory = new MqttFactory();
        _client = _factory.CreateMqttClient();

        _client.ApplicationMessageReceivedAsync += e =>
        {
            var topic = e.ApplicationMessage.Topic;
            var payload = Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment);
            var time = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss");
            var entry = $"[{time}] Topic: {topic} | Payload: {payload}";

            lock (_lock)
            {
                _messages.Add(entry);
            }

            Console.WriteLine(entry);
            return Task.CompletedTask;
        };
    }

    public async Task ConnectAsync(string broker, int port)
    {
        var options = new MqttClientOptionsBuilder().WithTcpServer(broker, port).WithClientId("restapi" + Random.Shared.Next(1000, 9999)).Build();
        await _client.ConnectAsync(options, CancellationToken.None);
        Console.WriteLine("Connected to broker");
    }

    public async Task SubscribeAsync(string topic)
    {
        var filter = new MqttTopicFilterBuilder().WithTopic(topic).WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce).Build();

        await _client.SubscribeAsync(filter);
    }

    public async Task PublishAsync(string topic, string payload, int qos)
    {
        var message = new MqttApplicationMessageBuilder().WithTopic(topic).WithPayload(payload).WithQualityOfServiceLevel((MqttQualityOfServiceLevel)qos).Build();
        await _client.PublishAsync(message, CancellationToken.None);
    }

    public List<string> GetMessages()
    {
        lock (_lock)
        {
            return new List<string>(_messages);
        }
    }
}

