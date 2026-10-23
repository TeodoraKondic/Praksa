using System.Text;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;

var factory = new MqttFactory();
var client = factory.CreateMqttClient();
var exiting = false;

Console.WriteLine("Choose QoS level (0/1/2):");
var qosLevel = Console.ReadLine()?.Trim();
MqttQualityOfServiceLevel chosenLevel;
switch (qosLevel)
{
    case "0":
        chosenLevel = MqttQualityOfServiceLevel.AtMostOnce;
        break;
    case "1":
        chosenLevel = MqttQualityOfServiceLevel.AtLeastOnce;
        break;
    case "2":
        chosenLevel = MqttQualityOfServiceLevel.ExactlyOnce;
        break;
    default: throw new ArgumentException("Invalid QoS level.");
}

Console.WriteLine("Please enter a topic:");
var inputTopic = Console.ReadLine();
if (string.IsNullOrWhiteSpace(inputTopic))
{
    inputTopic = "test/demo";
    Console.WriteLine("No topic entered, using test/demo.");
}

var options = new MqttClientOptionsBuilder().WithTcpServer("localhost", 1883).WithClientId("console-subscriber").Build();
var topicFilter = new MqttTopicFilterBuilder().WithTopic(inputTopic).WithQualityOfServiceLevel(chosenLevel).Build();


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
    if (exiting)
        return;

    Console.WriteLine("No connection with broker.");
    for (int i = 0; i < 3; i++)
    {
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(2));
            await client.ConnectAsync(options, CancellationToken.None);
            await client.SubscribeAsync(topicFilter);
            break;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Attempt {i + 1} failed: {ex.Message}");
        }
    }
};



try
{
    await client.ConnectAsync(options, CancellationToken.None);
    Console.WriteLine("Connected");
}
catch (Exception ex)
{
    Console.WriteLine($"Connection error: {ex.Message}");
    return;
}

try
{
    await client.SubscribeAsync(topicFilter);
}
catch (Exception ex)
{
    Console.WriteLine($"Subscription error: {ex.Message}");
}


Console.WriteLine("Publish mode. Type exit as topic to quit.");
await PublishMessages(client);

exiting = true;


await client.DisconnectAsync();
Console.WriteLine("Disconnected");

static async Task PublishMessages(IMqttClient client)
{
    while (true)
    {
        Console.WriteLine("Please enter a topic (or type 'exit' to quit):");
        var topic = Console.ReadLine();
        if (topic != null && topic.Trim().ToLower() == "exit")
            return;

        if (string.IsNullOrWhiteSpace(topic))
        {
            Console.WriteLine("Topic cannot be empty.");
            continue; 
        }

        Console.Write("Message: ");
        var text = Console.ReadLine();
        if (text == null)
        {
            text = "";
        }

        MqttQualityOfServiceLevel level = MqttQualityOfServiceLevel.AtMostOnce;
        var validLevel = false;
        while (!validLevel)
        {
            Console.WriteLine("Choose QoS level (0/1/2):");
            var qos = Console.ReadLine()?.Trim();
            switch (qos)
            {
                case "0":
                    level = MqttQualityOfServiceLevel.AtMostOnce;
                    validLevel = true;
                    break;
                case "1":
                    level = MqttQualityOfServiceLevel.AtLeastOnce;
                    validLevel = true;
                    break;
                case "2":
                    level = MqttQualityOfServiceLevel.ExactlyOnce;
                    validLevel = true;
                    break;
                default:
                    Console.WriteLine("Invalid QoS. Enter 0, 1 or 2.");
                    break;

            }
        }

        try
        {
            var message = new MqttApplicationMessageBuilder().WithTopic(topic.Trim()).WithPayload(text).WithQualityOfServiceLevel(level).Build();

            await client.PublishAsync(message, CancellationToken.None);

            var time = DateTime.Now.ToString("HH:mm:ss");
            Console.WriteLine($"[{time}]\nSent to {topic.Trim()}\n(QoS {(int)level}): {text}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Could not send message: {ex.Message}");
        }


    }


}
