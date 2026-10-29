using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var connectionString = builder.Configuration.GetConnectionString("DefaultConnection") ?? throw new InvalidOperationException("Missing connection string.");


builder.Services.Configure<MqttSettings>(builder.Configuration.GetSection("Mqtt"));
ServerVersion? serverVersion = null;
builder.Services.AddDbContext<AppDbContext>(options =>
{
    serverVersion ??= ServerVersion.AutoDetect(connectionString);
    options.UseMySql(connectionString, serverVersion);
});

builder.Services.AddSingleton<IMqttService, MqttService>();
builder.Services.AddSingleton(new RedisService("localhost:6380"));
builder.Services.AddHostedService<MqttReconnectService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
