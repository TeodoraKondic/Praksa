public interface IMessageLogger
{
    Task LogMessageAsync(string topic, string payload, int qos, bool retain);
}