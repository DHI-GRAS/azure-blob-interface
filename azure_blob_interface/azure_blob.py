import os
from pathlib import Path
from typing import Optional

from azure_blob_interface.storage import StorageDriver


class AzureStorageDriver(StorageDriver):
    def __init__(self, container: str, **kwargs):
        self.container = self.get_container(container, **kwargs)
        self.block_blob_service = self.get_block_blob_service(**kwargs)
        pass

    def _ensure_exists(self, blob_service, container_name, blob_name):
        if not blob_service.exists(container_name, blob_name):
            raise IOError(
                "Blob does n:wot exist: "
                f"container_name:{container_name}, blob_name:{blob_name}"
            )

    def download(
        self,
        prefix: str,
        path_local: Optional[Path] = None,
        overwrite: bool = False,
        **kwargs,
    ):
        """The data will be downloaded to path_local
        Assumes the data is in the working directory

        Parameters
        ----------
        prefix: str
            A prefix string, indiciting which files to download.
        overwrite: bool
            Whether to overwrite existing data.
        """
        if not path_local:
            path_local = Path()

        blob_file_paths = self.list_files(prefix, recursive=True)
        for blob_file_path in blob_file_paths:
            filename = Path(blob_file_path)
            out_path = path_local / filename
            out_path.parent.mkdir(exist_ok=True, parents=True)

            if not overwrite and out_path.exists():
                continue
            print("Downloading: ", out_path)
            with open(str(out_path), "wb") as of:
                blob_object = self.container.download_blob(
                    str(filename), max_concurrency=10, timeout=3000, **kwargs
                )
                blob_object.readinto(of)

    def get_block_blob_service(self, **kwargs):
        from azure.storage.blob import BlobServiceClient

        account_url = os.getenv("ACCOUNT_URL")
        block_blob_service = BlobServiceClient.from_connection_string(
            account_url, retry_total=0, **kwargs
        )
        return block_blob_service

    def get_container(self, container: str, **kwargs):
        return self.get_block_blob_service(**kwargs).get_container_client(container)

    def upload(
        self,
        path_local: Optional[Path] = None,
        path_upload: Optional[Path] = None,
        overwrite: bool = True,
        carry: Optional[Path] = None,
        **kwargs,
    ):
        """The data will up loaded to dir path_upload from dir path_local
        Assumes the data is in the working directory.

        Parameters
        ----------
        path_local: pathlib.Path
            Absolute or relative local path to SAFE
        path_upload: pathlib.Path
            Absolute or relative blob path to SAFE
        overwrite: bool
            if the data to be uploaded should be overridden or not
        carry: pathlib.Path
            special variable for carrying the postfix of path_local recursively.
            Should not be set manually.
        """
        carry = carry if carry else Path()
        root_file = path_local / carry
        if root_file.is_dir():
            for child_file in root_file.glob("*"):
                self.upload(
                    path_local,
                    path_upload,
                    overwrite,
                    child_file.relative_to(path_local),
                    **kwargs,
                )
        else:
            last_dir = Path(path_local.name) if path_local.is_dir() else Path()
            self._upload_file(
                root_file,
                path_upload / last_dir / carry.parent / root_file.name,
                overwrite,
                **kwargs,
            )

    def _upload_file(
        self, path_local: Path, path_upload: Path, overwrite: bool, **kwargs
    ):
        if not overwrite and self.exists(path_upload):
            return
        with open(path_local, "rb") as of:
            self.container.upload_blob(
                data=of.read(),
                blob_type="BlockBlob",
                name=str(path_upload),
                overwrite=True,
                max_concurrency=10,
                **kwargs,
            )

    def exists(self, blob_path: str):
        return bool(list(self.container.list_blobs(name_starts_with=blob_path)))

    def delete(self, prefix: str):
        blob_file_paths = self.container.list_blobs(name_starts_with=prefix)
        for file_path in blob_file_paths:
            self.container.delete_blob(file_path)

    def list_files(self, prefix: str, glob: str = None, recursive: bool = False):
        """Assumes prefix is a directory and list all files and directories below it.
        If recursive is set, glob is ignored."""

        prefix_with_slash = f"{str(Path(prefix))}/"

        if recursive:
            files = [
                Path(blob_path["name"])
                for blob_path in self.container.list_blobs(
                    name_starts_with=prefix, include="metadata"
                )
            ]

            if glob:
                files = [path for path in files if path.match(glob)]

            return sorted(files)

        files = [
            Path(f["name"])
            for f in self.container.walk_blobs(
                name_starts_with=prefix_with_slash, delimiter="/"
            )
        ]

        if not files:
            # The file listed is a blob and NOT a prefix
            if self.exists(prefix):
                return [prefix]
            else:
                return []

        if glob:
            files = [path for path in files if path.match(glob)]

        return sorted(files)

    def rename(self):
        pass
