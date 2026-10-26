public class CompositeMessageLogger : IMessageLogger
{
    private List<IMessageLogger> _loggers;

    public CompositeMessageLogger(params IMessageLogger[] loggers)
    {
        _loggers=new List<IMessageLogger>(loggers);
    }

    public async Task LogMessageAsync(string topic, string payload, int qos, bool retain)
    {
        foreach(var logger in _loggers)
        {
            await logger.LogMessageAsync(topic,payload,qos,retain);
        }
    }
}