using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace KafkaDemo.Migrations
{
    /// <inheritdoc />
    public partial class logDescripton : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<string>(
                name: "Description",
                table: "UserLoggedInEvents",
                type: "character varying(10240)",
                maxLength: 10240,
                nullable: false,
                defaultValue: "");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "Description",
                table: "UserLoggedInEvents");
        }
    }
}
