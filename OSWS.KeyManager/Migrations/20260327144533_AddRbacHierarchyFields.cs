using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace OSWS.KeyManager.Migrations
{
    /// <inheritdoc />
    public partial class AddRbacHierarchyFields : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "RoleInheritances",
                columns: table => new
                {
                    ParentRoleId = table.Column<int>(type: "integer", nullable: false),
                    ChildRoleId = table.Column<int>(type: "integer", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_RoleInheritances", x => new { x.ParentRoleId, x.ChildRoleId });
                    table.ForeignKey(
                        name: "FK_RoleInheritances_Roles_ChildRoleId",
                        column: x => x.ChildRoleId,
                        principalTable: "Roles",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_RoleInheritances_Roles_ParentRoleId",
                        column: x => x.ParentRoleId,
                        principalTable: "Roles",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_RoleInheritances_ChildRoleId",
                table: "RoleInheritances",
                column: "ChildRoleId");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "RoleInheritances");
        }
    }
}
