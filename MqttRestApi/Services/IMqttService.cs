public interface IMqttService
{
    bool IsConnected { get; }
    Task ConnectAsync(string broker, int port);
    Task SubscribeAsync(string topic, int qos);
    Task PublishAsync(string topic, string payload, int qos);
    List<string> GetMessages();
}