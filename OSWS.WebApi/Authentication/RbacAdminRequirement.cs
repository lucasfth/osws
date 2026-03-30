using Microsoft.AspNetCore.Authorization;
using OSWS.WebApi.Services;

namespace OSWS.WebApi.Authentication;

public class RbacAdminRequirement : IAuthorizationRequirement { }

/// <summary>
///     Satisfies <see cref="RbacAdminRequirement" /> by checking <c>User.IsRbacAdmin</c>
///     in the database via <see cref="CurrentUser" />.
///     Reuses the per-request cached DB lookup so no extra round trip is made
///     on top of what <see cref="CurrentUser" /> already does.
/// </summary>
public class RbacAdminHandler(CurrentUser currentUser)
    : AuthorizationHandler<RbacAdminRequirement>
{
    protected override async Task HandleRequirementAsync(
        AuthorizationHandlerContext context,
        RbacAdminRequirement requirement
    )
    {
        var user = await currentUser.ResolveAsync();
        if (user?.IsRbacAdmin == true)
            context.Succeed(requirement);
    }
}
