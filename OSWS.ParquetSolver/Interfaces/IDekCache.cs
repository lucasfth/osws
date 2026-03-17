namespace OSWS.ParquetSolver.Interfaces;

/// <summary>
/// Abstraction over the Data Encryption Key (DEK) cache.
/// Implementations: <see cref="Helpers.DekCache"/> (in-memory) and
/// <see cref="Helpers.RedisDekCache"/> (Redis).
/// </summary>
public interface IDekCache
{
    /// <summary>Try to retrieve a cached decrypted DEK.</summary>
    bool TryGet(string kekId, out byte[]? dek);

    /// <summary>Store a decrypted DEK in the cache.</summary>
    void Set(string kekId, byte[] dek);

    /// <summary>Remove all cached DEKs.</summary>
    void Clear();

    /// <summary>Current number of cached DEKs.</summary>
    int Count { get; }
}
