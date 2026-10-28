using Microsoft.EntityFrameworkCore;

public class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options)
    {
        
    }

    public DbSet<MqttMessage> Messages {get; set;} = null!;
    public DbSet<Subscription> Subscriptions {get; set;}= null!;
}