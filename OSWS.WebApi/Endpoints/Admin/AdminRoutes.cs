namespace OSWS.WebApi.Endpoints.Admin;

public static class AdminRoutes
{
    public static void MapAdminRoutes(this IEndpointRouteBuilder app)
    {
        var admin = app.MapGroup("/api/admin").RequireAuthorization("AdminPolicy");
        admin.MapRoleRoutes();
        admin.MapUserRoutes();
        admin.MapColumnRoutes();
    }
}
