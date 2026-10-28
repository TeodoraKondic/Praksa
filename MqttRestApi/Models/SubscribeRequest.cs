using System.ComponentModel.DataAnnotations;

public class SubscribeRequest
{
    [Required]
    public string Topic { get; set; } = "";
    
    [Range(0, 2)]
    public int Qos { get; set; } = 1;
}