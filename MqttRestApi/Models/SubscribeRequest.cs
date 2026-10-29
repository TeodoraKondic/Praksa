using System.ComponentModel.DataAnnotations;

public class SubscribeRequest
{
    [Required]
    [MaxLength(200)]
    public string Topic { get; set; } = "";

    [Range(0, 2)]
    public int Qos { get; set; } = 1;
}