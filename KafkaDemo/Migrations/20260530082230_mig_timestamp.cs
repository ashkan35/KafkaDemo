using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace KafkaDemo.Migrations
{
    /// <inheritdoc />
    public partial class mig_timestamp : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AlterColumn<DateTime>(
                name: "LoggedInAt",
                table: "UserLoggedInEvents",
                type: "timestamp without time zone",
                nullable: false,
                oldClrType: typeof(DateTime),
                oldType: "timestamp with time zone");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AlterColumn<DateTime>(
                name: "LoggedInAt",
                table: "UserLoggedInEvents",
                type: "timestamp with time zone",
                nullable: false,
                oldClrType: typeof(DateTime),
                oldType: "timestamp without time zone");
        }
    }
}
