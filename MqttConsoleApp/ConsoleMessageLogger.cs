public class ConsoleMessageLogger : IMessageLogger
{
    public Task LogMessageAsync(string topic, string payload, int qos, bool retain)
    {
        var time = DateTime.Now.ToString("HH:mm:ss");
        Console.WriteLine($"[{time}] Topic: {topic}, Payload: {payload}");
        return Task.CompletedTask;
    }
}
