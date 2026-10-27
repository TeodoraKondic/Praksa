public class FileMessageLogger :IMessageLogger
{
    private readonly string _logFilePath;
    public FileMessageLogger(string logFilePath)
    {
        _logFilePath=logFilePath;
    }

    public async Task LogMessageAsync(string topic, string payload, int qos, bool retain)
    {
        var timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss");
        var entry = $"[{timestamp}] Topic: {topic} | Payload: {payload} | QoS: {qos} | Retain: {retain.ToString().ToLower()}";
        await File.AppendAllTextAsync(_logFilePath, entry + Environment.NewLine);
    }
}