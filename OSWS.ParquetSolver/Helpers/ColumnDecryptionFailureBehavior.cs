namespace OSWS.ParquetSolver.Helpers;

public enum ColumnDecryptionFailureBehavior
{
    Throw = 0,
    DummyValues = 1,
    CopyEncrypted = 2,
}
