# Azure-blob-interface

This repo is generic rewrite of the [ccc-devkit](https://github.com/DHI-GRAS/ccc-devkit) repo. Allows simpler interfacing with Azure storage containers with some satellite specific filepath resolution.


## Usage
Set the environment variable `ACCOUNT_URL` to the connection string of the storage account to be used. Note the quotes around the string.
```
export ACCOUNT_URL="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
```

Use the package as follows
```python
import azure_blob_interface
import pathlib


driver = azure_blob_interface.AzureStorageDriver("<container name>")

# Upload a file
upload_source = pathlib.Path("<local file path>")
upload_destination_dir = pathlib.Path("<empty string for root dir. Otherwise path to remote dir>")
driver.upload(upload_source, upload_destination_dir)

# Download a file
download_source = pathlib.Path("<remote file path>")
download_destination_dir = pathlib.Path("<empty string for current dir. Otherwise path to local dir>")
driver.upload(download_source, download_destination_dir)
```
