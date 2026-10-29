using System.Text;
using Microsoft.EntityFrameworkCore;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;
using MySqlConnector;

public class MqttService : IMqttService
{
    private readonly IMqttClient _client;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly RedisService _redisService;

    public bool IsConnected => _client.IsConnected;

    public MqttService(IServiceScopeFactory scopeFactory, RedisService redisService)
    {
        _scopeFactory = scopeFactory;
        _redisService = redisService;
        var factory = new MqttFactory();
        _client = factory.CreateMqttClient();

        _client.ApplicationMessageReceivedAsync += async e =>
        {
            var topic = e.ApplicationMessage.Topic;
            var payload = Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment);
            var qos = (int)e.ApplicationMessage.QualityOfServiceLevel;
            var retain = e.ApplicationMessage.Retain;
            var receivedAt = DateTime.UtcNow;

            Console.WriteLine($"[{receivedAt:yyyy-MM-dd HH:mm:ss}] Topic:{topic} | Payload:{payload}");

            var dbTask=Task.Run(async () =>
            {
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
            });
            var redisTask=Task.Run(async()=>
            {
                try
                {
                    await _redisService.SaveMessageAsync(topic, payload);
                    await _redisService.IncrementTopicCountAsync(topic);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Could not save message to Redis: {ex.Message}");
                }
            });

            await Task.WhenAll(dbTask, redisTask);
        };
    }

    public async Task ConnectAsync(string broker, int port)
    {
        var options = new MqttClientOptionsBuilder().WithTcpServer(broker, port).WithClientId("restapi-" + Guid.NewGuid()).Build();

        await _client.ConnectAsync(options, CancellationToken.None);

        try
        {
            await RestoreSubscriptionsAsync();
        }
        catch
        {
            await _client.DisconnectAsync();
            throw;
        }
        Console.WriteLine("Connected to broker");

    }

    public async Task SubscribeAsync(string topic, int qos)
    {
        await SaveSubscriptionAsync(topic, qos);
        await SubscribeToBrokerAsync(topic, qos);
    }

    public async Task PublishAsync(string topic, string payload, int qos)
    {
        var message = new MqttApplicationMessageBuilder().WithTopic(topic).WithPayload(payload).WithQualityOfServiceLevel((MqttQualityOfServiceLevel)qos).Build();
        await _client.PublishAsync(message, CancellationToken.None);
    }

    public async Task<List<MqttMessage>> GetMessagesAsync()
    {
        using var scope = _scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
        return await db.Messages.AsNoTracking().OrderByDescending(m => m.ReceivedAt).Take(100).ToListAsync();
    }

    private async Task SubscribeToBrokerAsync(string topic, int qos)
    {
        var filter = new MqttTopicFilterBuilder().WithTopic(topic).WithQualityOfServiceLevel((MqttQualityOfServiceLevel)qos).Build();

        await _client.SubscribeAsync(filter);
    }

    private async Task RestoreSubscriptionsAsync()
    {
        using var scope = _scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
        var subscriptions = await db.Subscriptions.ToListAsync();

        foreach (var sub in subscriptions)
        {
            await SubscribeToBrokerAsync(sub.Topic, sub.QoS);
        }

    }

    private async Task SaveSubscriptionAsync(string topic, int qos)
    {
        using var scope = _scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
        var subscription = await db.Subscriptions.FirstOrDefaultAsync(s => s.Topic == topic);

        if (subscription == null)
        {
            db.Subscriptions.Add(new Subscription { Topic = topic, QoS = qos, CreatedAt = DateTime.UtcNow });
        }
        else
        {
            subscription.QoS = qos;
        }

        try
        {
            await db.SaveChangesAsync();
        }
        catch (DbUpdateException ex) when (ex.InnerException is MySqlException { ErrorCode: MySqlErrorCode.DuplicateKeyEntry })
        {

        }
    }
}

