
using MQTTnet;
using MQTTnet.Protocol;


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


var mqttClient= new MqttSubscriberClient(clientName,chosenLevel);
if(! await mqttClient.ConnectAsync())
    return;



await mqttClient.SubscribeTo("test/#");

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
            await PublishMessages(mqttClient);
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
                await mqttClient.SubscribeTo(newTopic);
            }
            break;
        case "q":
            running = false;
            break;
        case "list":
            Console.WriteLine("Currently subscribed to topics:");
            foreach (var listedTopic in mqttClient.Topics)
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


await mqttClient.DisconnectAsync();
Console.WriteLine("Disconnected");


static async Task PublishMessages(MqttSubscriberClient mqttClient)
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

            await mqttClient.PublishAsync(topic.Trim(), text,level);

            var time = DateTime.Now.ToString("HH:mm:ss");
            Console.WriteLine($"[{time}]\nSent to {topic.Trim()}\n(QoS {(int)level}): {text}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Could not send message: {ex.Message}");
        }


    }


}
