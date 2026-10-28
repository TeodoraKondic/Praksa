using System.ComponentModel.DataAnnotations;

public class PublishRequest
{
    [Required]
    public string Topic { get; set; } = "";
    [Required]
    public string Payload { get; set; } = "";
    [Range(0,2)]
    public int Qos { get; set; }
}