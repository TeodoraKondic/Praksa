using System.Text;
using Microsoft.EntityFrameworkCore;

//using Microsoft.EntityFrameworkCore;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;

public class MqttService : IMqttService
{
    private readonly IMqttClient _client;
    private readonly List<string> _messages = new List<string>();
    private readonly object _lock = new object();
     private readonly IServiceScopeFactory _scopeFactory;

    public bool IsConnected => _client.IsConnected;

    public MqttService(IServiceScopeFactory scopeFactory)
    {
        _scopeFactory=scopeFactory;
        var factory = new MqttFactory();
        _client = factory.CreateMqttClient();

        _client.ApplicationMessageReceivedAsync += async e =>
        {
            var topic = e.ApplicationMessage.Topic;
            var payload = Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment);
            var time = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss");
            var qos = (int)e.ApplicationMessage.QualityOfServiceLevel;
            var retain = e.ApplicationMessage.Retain;
            var receivedAt = DateTime.UtcNow;
            var entry = $"[{time}] Topic: {topic} | Payload: {payload}";

            lock (_lock)
            {
                _messages.Add(entry);
            }

            Console.WriteLine(entry);

            try
            {
                using var scope = _scopeFactory.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

                var message = new MqttMessage
                {
                    Topic = topic,
                    Payload = payload,
                    QoS = qos,
                    Retain = retain,
                    ReceivedAt = receivedAt
                };

                db.Messages.Add(message);
                await db.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Could not save message: {ex.Message}");
            }
            
        };
    }

    public async Task ConnectAsync(string broker, int port)
    {
        var options = new MqttClientOptionsBuilder().WithTcpServer(broker, port).WithClientId("restapi-" + Guid.NewGuid()).Build();
        
        await _client.ConnectAsync(options, CancellationToken.None);    
        Console.WriteLine("Connected to broker");

        await RestoreSubscriptionsAsync();
       
    }

    public async Task SubscribeAsync(string topic, int qos)
    {
         using (var scope = _scopeFactory.CreateScope())
        {
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
            var subscription = await db.Subscriptions.FirstOrDefaultAsync(s => s.Topic == topic);

            if (subscription == null)
            {
                db.Subscriptions.Add(new Subscription
                {
                    Topic = topic,
                    QoS = qos,
                    CreatedAt = DateTime.UtcNow
                });
            }
            else
            {
                subscription.QoS = qos;
            }

            await db.SaveChangesAsync();
        }

        await SubscribeToBrokerAsync(topic, qos);
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

    private async Task RestoreSubscriptionsAsync()
    {
        try
        {
            using var scope = _scopeFactory.CreateScope();
            var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
            var subscriptions = await db.Subscriptions.ToListAsync();

            foreach (var sub in subscriptions)
            {
                await SubscribeToBrokerAsync(sub.Topic, sub.QoS);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Could not restore subscriptions: {ex.Message}");
        }
    }


}

