using System.Text;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;

var factory = new MqttFactory();
var client = factory.CreateMqttClient();
var exiting = false;
var reconnecting = false;
var topics = new List<string>();

Console.WriteLine("Enter client name:");
var clientName = Console.ReadLine()?.Trim();
if (string.IsNullOrWhiteSpace(clientName))
{
    clientName = "app" + Random.Shared.Next(1000, 9999);
}

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


var options = new MqttClientOptionsBuilder().WithTcpServer("localhost", 1883).WithClientId(clientName).Build();


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


await SubscribeTo("test/#");

static void PrintHelp()
{
    Console.WriteLine("Commands");
    Console.WriteLine("  p     publish a message");
    Console.WriteLine("  s     subscribe to a new topic");
    Console.WriteLine("  list  show subscribed topics");
    Console.WriteLine("  help  show this list");
    Console.WriteLine("  q     quit");
}

PrintHelp();
var running = true;
while (running)
{
    Console.Write("> ");
    var command = Console.ReadLine()?.Trim().ToLower();

    switch (command)
    {
        case "p":
            await PublishMessages(client);
            break;
        case "s":
            Console.WriteLine("Topic for subscribe");
            var newTopic = Console.ReadLine()?.Trim();
            if (string.IsNullOrWhiteSpace(newTopic))
            {
                Console.WriteLine("Topic cannot be empty.");
                continue;
            }
            else
            {
                await SubscribeTo(newTopic);
            }
            break;
        case "q":
            running = false;
            break;
        case "list":
            Console.WriteLine("Currently subscribed to topics:");
            foreach (var listedTopic in topics)
            {
                Console.WriteLine($" - {listedTopic}");
            }
            break;
        case "help":
            PrintHelp();
            break;
        default:
            Console.WriteLine("Unknown command. Type help.");
            break;
    }
}


exiting = true;

await client.DisconnectAsync();
Console.WriteLine("Disconnected");

async Task SubscribeTo(string topic)
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
