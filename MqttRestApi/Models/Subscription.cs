using System.ComponentModel.DataAnnotations;
using Microsoft.EntityFrameworkCore;

[Index(nameof(Topic), IsUnique = true)]
public class Subscription
{
    [Key]
    public int Id { get; set; }
    [Required]
    [MaxLength(200)]
    public string Topic { get; set; } = "";
    public int QoS { get; set; }
    public DateTime CreatedAt { get; set; }

}