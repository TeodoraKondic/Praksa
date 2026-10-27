public class PublishRequest
{
    public string Topic { get; set; } = "";
    public string Payload { get; set; } = "";
    public int Qos { get; set; }
}