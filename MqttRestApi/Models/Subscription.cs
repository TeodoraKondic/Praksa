using System.ComponentModel.DataAnnotations;

public class Subscription
{
    [Key]
    public int Id { get; set; }
    [Required]
    public string Topic { get; set; } = "";
    public int QoS { get; set; }
    public DateTime CreatedAt { get; set; }

}