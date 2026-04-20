using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace OSWS.KeyManager.Migrations
{
    /// <inheritdoc />
    public partial class AddUserIsRbacAdmin : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<bool>(
                name: "IsRbacAdmin",
                table: "Users",
                type: "boolean",
                nullable: false,
                defaultValue: false
            );
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(name: "IsRbacAdmin", table: "Users");
        }
    }
}
