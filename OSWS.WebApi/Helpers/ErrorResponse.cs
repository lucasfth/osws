using OSWS.Library.Helpers;

namespace OSWS.WebApi.Helpers;

public static class ErrorResponse
{
    /// <summary>
    /// Returns a sanitized error result. In Development, includes full exception details.
    /// In production, returns only the safe message.
    /// </summary>
    public static IResult InternalError(
        string safeMessage,
        Exception ex,
        IWebHostEnvironment env)
    {
        if (env.IsDevelopment())
            return Results.Text(
                ParamValidation.CreateErrorJson($"{safeMessage}: {ex.Message}"),
                "application/json",
                statusCode: 500);

        return Results.Text(
            ParamValidation.CreateErrorJson(safeMessage),
            "application/json",
            statusCode: 500);
    }
}
