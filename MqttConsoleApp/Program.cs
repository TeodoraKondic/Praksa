using MQTTnet.Protocol;


Console.WriteLine("Enter client name:");
var clientName = Console.ReadLine()?.Trim();
if (string.IsNullOrWhiteSpace(clientName))
{
    clientName = "app" + Random.Shared.Next(1000, 9999);
}

Console.WriteLine("Choose QoS level (0/1/2):");

MqttQualityOfServiceLevel chosenLevel;
while (true)
{
    var qosLevel = Console.ReadLine()?.Trim();
    if (qosLevel == null)
    {
        return;
    }
    if (TryParseQoS(qosLevel, out chosenLevel))
    {
        break;
    }
    Console.WriteLine("Invalid QoS. Enter 0, 1 or 2.");
    break;
}

const string logFileName = "mqtt_messages.log";
Console.WriteLine("Choose logger (1 = console, 2 = file, 3 = both):");
IMessageLogger? logger = null;
while (logger == null)
{
    var loggerChoice = Console.ReadLine()?.Trim();
    if (loggerChoice == null)
    {
        return;
    }
    switch (loggerChoice)
    {
        case "1":
            logger = new ConsoleMessageLogger();
            break;
        case "2":
            logger = new FileMessageLogger(logFileName);
            break;
        case "3":
            logger = new CompositeMessageLogger(new ConsoleMessageLogger(), new FileMessageLogger(logFileName));
            break;
        default:
            Console.WriteLine("Invalid choice. Enter 1, 2 or 3.");
            break;
    }
}
var mqttClient = new MqttClientService(clientName, chosenLevel, logger);

if (!await mqttClient.ConnectAsync())
{
    return;
}




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

static bool TryParseQoS(string? input, out MqttQualityOfServiceLevel level)
{
    switch (input)
    {
        case "0":
            level = MqttQualityOfServiceLevel.AtMostOnce;
            return true;
        case "1":
            level = MqttQualityOfServiceLevel.AtLeastOnce;
            return true;
        case "2":
            level = MqttQualityOfServiceLevel.ExactlyOnce;
            return true;
        default:
            level = MqttQualityOfServiceLevel.AtMostOnce;
            return false;
    }
}

PrintHelp();
var running = true;
while (running)
{
    Console.Write("> ");
    var command = Console.ReadLine()?.Trim().ToLower();
    if (command == null)
    {
        break;
    }

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


static async Task PublishMessages(MqttClientService mqttClient)
{
    while (true)
    {
        Console.WriteLine("Please enter a topic (or type 'exit' to quit):");
        var topic = Console.ReadLine();
        if (topic == null)
        {
            return;
        }
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
        while (true)
        {
            Console.WriteLine("Choose QoS level (0/1/2):");
            var qos = Console.ReadLine()?.Trim();
            if (qos == null)
            {
                return;
            }
            if (TryParseQoS(qos, out level))
            {
                break;
            }
            Console.WriteLine("Invalid QoS. Enter 0, 1 or 2.");
        }


        try
        {
            await mqttClient.PublishAsync(topic.Trim(), text, level);

            var time = DateTime.Now.ToString("HH:mm:ss");
            Console.WriteLine($"[{time}]\nSent to {topic.Trim()}\n(QoS {(int)level}): {text}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Could not send message: {ex.Message}");
        }


    }


}
