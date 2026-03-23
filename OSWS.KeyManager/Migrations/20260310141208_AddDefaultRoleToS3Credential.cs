using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace OSWS.KeyManager.Migrations
{
    /// <inheritdoc />
    public partial class AddDefaultRoleToS3Credential : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<int>(
                name: "DefaultRoleId",
                table: "S3Credentials",
                type: "integer",
                nullable: true);

            migrationBuilder.CreateIndex(
                name: "IX_S3Credentials_DefaultRoleId",
                table: "S3Credentials",
                column: "DefaultRoleId");

            migrationBuilder.AddForeignKey(
                name: "FK_S3Credentials_Roles_DefaultRoleId",
                table: "S3Credentials",
                column: "DefaultRoleId",
                principalTable: "Roles",
                principalColumn: "Id");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropForeignKey(
                name: "FK_S3Credentials_Roles_DefaultRoleId",
                table: "S3Credentials");

            migrationBuilder.DropIndex(
                name: "IX_S3Credentials_DefaultRoleId",
                table: "S3Credentials");

            migrationBuilder.DropColumn(
                name: "DefaultRoleId",
                table: "S3Credentials");
        }
    }
}
