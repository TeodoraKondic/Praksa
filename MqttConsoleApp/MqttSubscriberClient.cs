using System.Text;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;

public class MqttSubscriberClient
{
    private readonly IMqttClient client;
    private readonly MqttClientOptions options;
    private readonly MqttQualityOfServiceLevel chosenLevel;
    private readonly List<string> topics = new List<string>();
    private bool exiting = false;
    private bool reconnecting = false;

    public IReadOnlyList<string> Topics => topics;

    public MqttSubscriberClient(string clientName, MqttQualityOfServiceLevel chosenLevel)
    {
        this.chosenLevel = chosenLevel;

        var factory = new MqttFactory();
        client = factory.CreateMqttClient();

        options = new MqttClientOptionsBuilder().WithTcpServer("localhost", 1883).WithClientId(clientName).Build();

        client.ApplicationMessageReceivedAsync += e =>
        {
            var topic = e.ApplicationMessage.Topic;
            var payload = Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment);
            var qos = (int)e.ApplicationMessage.QualityOfServiceLevel;
            var retain = e.ApplicationMessage.Retain;
            var time = DateTime.Now.ToString("HH:mm:ss");
            Console.WriteLine($"[{time}]\nTopic: {topic}\nMessage: {payload}\nQoS: {qos}\nRetain: {retain}");
            return Task.CompletedTask;
        };

        client.DisconnectedAsync += async e =>
        {
            if (exiting || reconnecting)
                return;

            reconnecting = true;

            Console.WriteLine("No connection with broker.");
            try
            {
                for (int i = 0; i < 3; i++)
                {
                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(2));
                        if (!client.IsConnected)
                            await client.ConnectAsync(options, CancellationToken.None);

                        foreach (var savedTopic in topics.ToList())
                        {
                            var savedFilter = new MqttTopicFilterBuilder().WithTopic(savedTopic).WithQualityOfServiceLevel(chosenLevel).Build();
                            await client.SubscribeAsync(savedFilter);
                        }
                        break;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Attempt {i + 1} failed: {ex.Message}");
                    }
                }
            }
            finally
            {
                reconnecting = false;
            }
        };
    }

    public async Task<bool> ConnectAsync()
    {
        try
        {
            await client.ConnectAsync(options, CancellationToken.None);
            Console.WriteLine("Connected");
            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Connection error: {ex.Message}");
            return false;
        }
    }

    public async Task SubscribeTo(string topic)
    {
        try
        {
            var filter = new MqttTopicFilterBuilder().WithTopic(topic).WithQualityOfServiceLevel(chosenLevel).Build();
            await client.SubscribeAsync(filter);
            if (!topics.Contains(topic))
                topics.Add(topic);

            Console.WriteLine($"Subscribed to: {topic} (QoS {(int)chosenLevel})");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Subscription error: {ex.Message}");
        }
    }

    public async Task PublishAsync(string topic, string text, MqttQualityOfServiceLevel level)
    {
        var message = new MqttApplicationMessageBuilder().WithTopic(topic).WithPayload(text).WithQualityOfServiceLevel(level).Build();

        await client.PublishAsync(message, CancellationToken.None);
    }

    public async Task DisconnectAsync()
    {
        exiting = true;

        await client.DisconnectAsync();
    }
}