public class MessageLogger
{
    public string _logFilePath {get;}
    public MessageLogger(string logFilePath)
    {
        _logFilePath=logFilePath;
    }

    public async Task LogMessageAsync(string topic, string payload, int qos, bool retain)
    {
        var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
        var entry = $"[{timestamp}] Topic: {topic} | Payload: {payload} | QoS: {qos} | Retain: {retain.ToString().ToLower()}";
        await File.AppendAllTextAsync(_logFilePath, entry + Environment.NewLine);
    }
}