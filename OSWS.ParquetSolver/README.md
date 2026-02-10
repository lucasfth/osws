# README

To view first row of a parquet file, you can use the `parquet head` command:

```bash
parquet head -n 1 file.parquet
```

When viewing unencrypted only the schema can be viewed:

```bash
parquet schema file.parquet
```
