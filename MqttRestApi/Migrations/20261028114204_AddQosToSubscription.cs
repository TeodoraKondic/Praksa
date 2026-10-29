using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace MqttRestApi.Migrations
{
    /// <inheritdoc />
    public partial class AddQosToSubscription : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<int>(
                name: "QoS",
                table: "Subscriptions",
                type: "int",
                nullable: false,
                defaultValue: 0);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "QoS",
                table: "Subscriptions");
        }
    }
}
