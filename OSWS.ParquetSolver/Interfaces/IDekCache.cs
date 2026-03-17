namespace OSWS.ParquetSolver.Interfaces;

/// <summary>
/// Abstraction over the Data Encryption Key (DEK) cache.
/// Implementations: <see cref="Helpers.DekCache"/> (in-memory with TTL) and
/// <see cref="Helpers.RedisDekCache"/> (Redis with TTL).
/// </summary>
/// <remarks>
/// TODO (RBAC): Once RBAC is implemented, TTL must be determined per-entry based on the
/// caller's role. Higher-privilege roles should use shorter TTLs to limit exposure window;
/// lower-privilege roles may use longer TTLs to reduce key-vault round-trips.
/// Search for "TODO (RBAC)" to find all related touch-points.
/// </remarks>
public interface IDekCache
{
    /// <summary>Try to retrieve a cached decrypted DEK. Returns false if missing or expired.</summary>
    bool TryGet(string kekId, out byte[]? dek);

    /// <summary>
    /// Store a decrypted DEK in the cache.
    /// TODO (RBAC): Accept a role/principal parameter to derive per-entry TTL.
    /// </summary>
    void Set(string kekId, byte[] dek);

    /// <summary>Evict all cached DEKs.</summary>
    void Clear();
}
