# erwl

Minimum viable data transfer command line tool.

## Install

Build with the required features enabled.

If you want to extract records from a local JSON file and load them into your S3 bucket in Parquet format, install them as follows:

```console
cargo install --git https://github.com/koji-m/erwl \
    --features reader-json,writer-parquet,extracter-file,loader-s3
```

## Run

If you want to execute as described above data transfer, execute as follows.

```console
erwl --input-file records.json --schema schema.json --batch-size 20000 \
    --compression snappy --s3-bucket my-bucket --key-prefix records_
```
