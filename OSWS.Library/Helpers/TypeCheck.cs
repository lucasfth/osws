namespace OSWS.Library.Helpers;

public static class TypeCheck
{
    public static bool IsParquetFile(string? key, string? contentType)
    {
        if (key?.EndsWith(".parquet", StringComparison.OrdinalIgnoreCase) == true)
            return true;

        return contentType?.Contains("parquet", StringComparison.OrdinalIgnoreCase) == true;
    }
}
