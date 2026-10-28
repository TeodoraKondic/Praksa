using System.Text;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;

public class MqttService : IMqttService
{
    private readonly IMqttClient _client;
    private readonly List<string> _messages = new List<string>();
    private readonly object _lock = new object();
    private readonly Dictionary<string, int> _topics = new Dictionary<string, int>();

    public bool IsConnected => _client.IsConnected;

    public MqttService()
    {
        var factory = new MqttFactory();
        _client = factory.CreateMqttClient();

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
        var options = new MqttClientOptionsBuilder().WithTcpServer(broker, port).WithClientId("restapi-"+ Guid.NewGuid()).Build();
        await _client.ConnectAsync(options, CancellationToken.None);
        Dictionary<string, int> savedTopics;
        lock (_lock)
        {
            savedTopics = new Dictionary<string, int>(_topics);
        }

        foreach (var saved in savedTopics)
        {
            await SubscribeToBrokerAsync(saved.Key, saved.Value);
        }
        Console.WriteLine("Connected to broker");
    }

    public async Task SubscribeAsync(string topic,int qos)
    {
        await SubscribeToBrokerAsync(topic, qos);

        lock (_lock)
        {
            _topics[topic] = qos;
        }
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

    private async Task SubscribeToBrokerAsync(string topic, int qos)
    {
        var filter = new MqttTopicFilterBuilder().WithTopic(topic).WithQualityOfServiceLevel((MqttQualityOfServiceLevel)qos).Build();

        await _client.SubscribeAsync(filter);
    }

}

