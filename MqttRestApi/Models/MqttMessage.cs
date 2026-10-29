using System.ComponentModel.DataAnnotations;

public class MqttMessage
{
    [Key]
    public int Id { get; set; }
    [Required]
    public string Topic { get; set; } = "";
    [Required]
    public string Payload { get; set; } = "";

    public int QoS { get; set; }
    public bool Retain { get; set; }
    public DateTime ReceivedAt { get; set; }
}