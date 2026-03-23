using Microsoft.AspNetCore.Authentication;

namespace OSWS.WebApi.Authentication;

public class SigV4AuthenticationOptions : AuthenticationSchemeOptions
{
    /// <summary>
    /// Maximum allowed difference (in seconds) between the timestamp in the request
    /// and the server's current UTC time. Requests outside this window are rejected.
    /// AWS default is 900 seconds (15 minutes); 300 is more conservative.
    /// </summary>
    public int MaxClockSkewSeconds { get; set; } = 300;
}
