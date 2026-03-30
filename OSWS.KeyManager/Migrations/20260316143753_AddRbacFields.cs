using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace OSWS.KeyManager.Migrations
{
    /// <inheritdoc />
    public partial class AddRbacFields : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropIndex(name: "IX_Keys_KeyVaultId", table: "Keys");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateIndex(
                name: "IX_Keys_KeyVaultId",
                table: "Keys",
                column: "KeyVaultId",
                unique: true
            );
        }
    }
}
