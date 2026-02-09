namespace OSWS.Library.Helpers;

/// <summary>
/// Helper class for validating parameters and generating JSON error responses for the OSWS Web API.
/// </summary>
public class ParamValidation
{
    /// <summary>
    /// Bucket names must be provided. Otherwise return a JSON error message indicating that the bucket name is required.
    /// </summary>
    public static string BucketNameIsRequired()
    {
        return CreateErrorJson("Bucket name is required");
    }

    /// <summary>
    /// Keys must be provided for certain operations. Returns a JSON error message.
    /// </summary>
    public static string KeyIsRequired()
    {
        return CreateErrorJson("Key is required for object PUT");
    }

    /// <summary>
    /// Creates a JSON error response with the given message.
    /// </summary>
    public static string CreateErrorJson(string message)
    {
        return "{" + "\"error\":" + JsonEscape(message) + "}";
    }

    /// <summary>
    /// Escapes a string value for use in JSON.
    /// </summary>
    public static string JsonEscape(string? s)
    {
        if (s == null)
            return "null";
        var sb = new System.Text.StringBuilder();
        sb.Append('"');
        foreach (var c in s)
        {
            switch (c)
            {
                case '\\':
                    sb.Append("\\\\");
                    break;
                case '"':
                    sb.Append("\\\"");
                    break;
                case '\b':
                    sb.Append("\\b");
                    break;
                case '\f':
                    sb.Append("\\f");
                    break;
                case '\n':
                    sb.Append("\\n");
                    break;
                case '\r':
                    sb.Append("\\r");
                    break;
                case '\t':
                    sb.Append("\\t");
                    break;
                default:
                    if (char.IsControl(c))
                        sb.Append("\\u").Append(((int)c).ToString("x4"));
                    else
                        sb.Append(c);
                    break;
            }
        }
        sb.Append('"');
        return sb.ToString();
    }
}
