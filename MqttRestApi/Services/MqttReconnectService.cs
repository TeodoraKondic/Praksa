using Microsoft.Extensions.Options;

public class MqttReconnectService : BackgroundService
{
    private readonly IMqttService _mqttService;
    private readonly MqttSettings _settings;

    public MqttReconnectService(IMqttService mqttService, IOptions<MqttSettings> options)
    {
        _mqttService = mqttService;
        _settings = options.Value;

    }
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
         while (!stoppingToken.IsCancellationRequested)
        {
            if (!_mqttService.IsConnected)
            {
                try
                {
                    await _mqttService.ConnectAsync(_settings.Broker, _settings.Port);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Could not connect to broker or restore subscriptions: {ex.Message}");
                }
            }

            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
        }
    }
}