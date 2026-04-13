namespace OSWS.ParquetSolver.Interfaces;

/// <summary>
/// Abstraction over the Data Encryption Key (DEK) cache.
/// Implementation: <see cref="Helpers.DekCache"/> (in-memory with role-based TTL).
/// </summary>
public interface IDekCache
{
    /// <summary>Try to retrieve a cached decrypted DEK. Returns false if missing or expired.</summary>
    bool TryGet(string kekId, out byte[]? dek);

    /// <summary>
    /// Store a decrypted DEK in the cache.
    /// </summary>
    /// <param name="kekId">Cache key identifying this DEK.</param>
    /// <param name="dek">The decrypted DEK bytes.</param>
    /// <param name="isAdmin">When true, uses the shorter admin TTL.</param>
    void Set(string kekId, byte[] dek, bool isAdmin = false);

    /// <summary>Evict all cached DEKs.</summary>
    void Clear();
}
