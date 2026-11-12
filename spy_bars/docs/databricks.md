Databricks provides a governed Unity Catalog landing location for later analysis of SPY bars; this step only uploads files.
Databricks Free Edition cannot reach MongoDB, Kafka, or landing files on this Mac through localhost, and its serverless compute is quota-limited.
The workaround is a local Python uploader that pushes completed JSON Lines files to a Unity Catalog Volume over HTTPS and records uploaded filenames locally.
