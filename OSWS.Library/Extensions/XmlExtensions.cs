using System.Globalization;
using System.Xml.Linq;
using Amazon.S3.Model;

namespace OSWS.Library.Extensions;

public static class XmlExtensions
{
    private static readonly XNamespace S3XmlNs = "http://s3.amazonaws.com/doc/2006-03-01/";

    private static string FormatS3Date(DateTime? dt) =>
        (dt ?? new DateTime())
            .ToUniversalTime()
            .ToString("yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture);

    private static XElement? CreateIfNotEmpty(XName name, string? value) =>
        string.IsNullOrEmpty(value) ? null : new XElement(name, value);

    private static XElement? CreateOwner(Owner? owner)
    {
        if (owner is null)
            return null;

        return new XElement(
            S3XmlNs + "Owner",
            new XElement(S3XmlNs + "DisplayName", owner.DisplayName ?? string.Empty),
            new XElement(S3XmlNs + "ID", owner.Id ?? string.Empty)
        );
    }

    private static XElement? CreateRestoreStatus(RestoreStatus? status)
    {
        if (status is null)
            return null;

        return new XElement(
            S3XmlNs + "RestoreStatus",
            new XElement(S3XmlNs + "IsRestoreInProgress", status.IsRestoreInProgress),
            status.RestoreExpiryDate is null
                ? null
                : new XElement(
                    S3XmlNs + "RestoreExpiryDate",
                    FormatS3Date(status.RestoreExpiryDate)
                )
        );
    }

    public static XDocument ToListObjectsV2Xml(this ListObjectsV2Response resp, string bucket)
    {
        var contents = resp.S3Objects?.Select(o => new XElement(
            S3XmlNs + "Contents",
            o.ChecksumAlgorithm?.Select(algo => new XElement(S3XmlNs + "ChecksumAlgorithm", algo)),
            CreateIfNotEmpty(S3XmlNs + "ChecksumType", o.ChecksumType),
            new XElement(S3XmlNs + "ETag", o.ETag ?? string.Empty),
            new XElement(S3XmlNs + "Key", o.Key ?? string.Empty),
            new XElement(S3XmlNs + "LastModified", FormatS3Date(o.LastModified)),
            CreateOwner(o.Owner),
            CreateRestoreStatus(o.RestoreStatus),
            new XElement(S3XmlNs + "Size", o.Size),
            new XElement(S3XmlNs + "StorageClass", o.StorageClass ?? "STANDARD")
        ));

        var commonPrefixes = resp.CommonPrefixes?.Select(p => new XElement(
            S3XmlNs + "CommonPrefixes",
            new XElement(S3XmlNs + "Prefix", p ?? string.Empty)
        ));

        return new XDocument(
            new XElement(
                S3XmlNs + "ListBucketResult",
                new XElement(
                    S3XmlNs + "IsTruncated",
                    resp.IsTruncated.ToString()?.ToLowerInvariant()
                ),
                contents,
                new XElement(S3XmlNs + "Name", bucket),
                new XElement(S3XmlNs + "Prefix", resp.Prefix ?? string.Empty),
                new XElement(S3XmlNs + "Delimiter", resp.Delimiter ?? string.Empty),
                new XElement(S3XmlNs + "MaxKeys", resp.MaxKeys),
                commonPrefixes,
                CreateIfNotEmpty(S3XmlNs + "EncodingType", resp.Encoding),
                new XElement(S3XmlNs + "KeyCount", resp.KeyCount),
                CreateIfNotEmpty(S3XmlNs + "ContinuationToken", resp.ContinuationToken),
                CreateIfNotEmpty(S3XmlNs + "NextContinuationToken", resp.NextContinuationToken),
                CreateIfNotEmpty(S3XmlNs + "StartAfter", resp.StartAfter)
            )
        );
    }

    public static XDocument ToListBucketsXml(this ListBucketsResponse resp)
    {
        var buckets = new XElement(
            S3XmlNs + "Buckets",
            resp.Buckets?.Select(b => new XElement(
                S3XmlNs + "Bucket",
                new XElement(S3XmlNs + "BucketArn", b.BucketArn ?? string.Empty),
                new XElement(S3XmlNs + "BucketRegion", b.BucketRegion ?? string.Empty),
                new XElement(S3XmlNs + "CreationDate", FormatS3Date(b.CreationDate)),
                new XElement(S3XmlNs + "Name", b.BucketName ?? string.Empty)
            ))
        );

        return new XDocument(
            new XElement(
                S3XmlNs + "ListAllMyBucketsResult",
                buckets,
                CreateOwner(resp.Owner),
                new XElement(S3XmlNs + "ContinuationToken", resp.ContinuationToken ?? string.Empty),
                new XElement(S3XmlNs + "Prefix", resp.Prefix ?? string.Empty)
            )
        );
    }
}
