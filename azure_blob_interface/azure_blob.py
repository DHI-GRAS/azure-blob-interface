import os
from pathlib import Path
from typing import Optional, Union, List
import logging

from azure.storage.blob import RehydratePriority
from azure.storage.blob import StandardBlobTier
from azure.core.exceptions import (
    ServiceRequestError,
    ServiceResponseError,
    HttpResponseError,
)

from azure_blob_interface.storage import StorageDriver


class AzureStorageDriver(StorageDriver):
    def __init__(
        self,
        container: str,
        logging_level=logging.ERROR,
        env_name: str = "ACCOUNT_URL",
        **kwargs,
    ):
        self.container = self.get_container(container, **kwargs)
        self.block_blob_service = self.get_block_blob_service(env_name, **kwargs)
        logging.getLogger("azure").setLevel(logging_level)

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
        retries: int = 1,
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
            tries = retries + 1
            for i in range(tries):
                try:
                    with open(str(out_path), "wb") as of:
                        blob_object = self.container.download_blob(
                            str(filename), max_concurrency=10, timeout=3000, **kwargs
                        )
                        blob_object.readinto(of)
                    break
                except (
                    ServiceRequestError,
                    ServiceResponseError,
                    HttpResponseError,
                ) as e:
                    if i + 1 == tries:
                        raise e
                    out_path.unlink(missing_ok=True)
                    continue

    def get_block_blob_service(self, env_name: str, **kwargs):
        from azure.storage.blob import BlobServiceClient

        account_url = os.getenv(env_name)
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
        retries: int = 1,
        **kwargs,
    ) -> Union[str, List[str]]:
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
            blob_urls = []
            for child_file in root_file.glob("*"):
                blob_url = self.upload(
                    path_local,
                    path_upload,
                    overwrite,
                    child_file.relative_to(path_local),
                    **kwargs,
                )
                blob_urls.append(blob_url)
            return blob_urls
        else:
            last_dir = Path(path_local.name) if path_local.is_dir() else Path()
            blob_url = self._upload_file(
                root_file,
                path_upload / last_dir / carry.parent / root_file.name,
                overwrite,
                retries=retries,
                **kwargs,
            )
            return blob_url

    def _upload_file(
        self,
        path_local: Path,
        path_upload: Path,
        overwrite: bool,
        retries: int,
        **kwargs,
    ):
        if not overwrite and self.exists(path_upload):
            return
        tries = retries + 1
        for i in range(tries):
            try:
                with open(path_local, "rb") as of:
                    blob_client = self.container.upload_blob(
                        data=of.read(),
                        blob_type="BlockBlob",
                        name=str(path_upload),
                        overwrite=True,
                        max_concurrency=10,
                        **kwargs,
                    )
                    break
            except (ServiceRequestError, ServiceResponseError, HttpResponseError) as e:
                if i + 1 == tries:
                    raise e
                continue
        return blob_client.url

    def exists(self, blob_path: str):
        return bool(list(self.container.list_blobs(name_starts_with=blob_path)))

    def delete(self, prefix: str):
        blob_file_paths = self.container.list_blobs(name_starts_with=prefix)
        for file_path in blob_file_paths:
            self.container.delete_blob(file_path)

    def list_files(
        self, prefix: str, glob: str = None, recursive: bool = False, retries: int = 1
    ):
        """Assumes prefix is a directory and list all files and directories below it.
        If recursive is set, glob is ignored."""

        prefix_with_slash = f"{str(Path(prefix))}/"

        tries = retries + 1
        for i in range(tries):
            try:
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
                break
            except (ServiceRequestError, ServiceResponseError, HttpResponseError) as e:
                if i + 1 == tries:
                    raise e
                continue

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

    def copy(
        self,
        src_path,
        dst_path,
        dst_container_name=None,
        dst_blob_tier=StandardBlobTier.HOT,
        rehydrate_priority=RehydratePriority.STANDARD,
        **kwargs,
    ):
        """Copy data from one blob to another in the same or different container. The source blob
           can be in archive tier.

           Can throw ResourceExistsError when source blob is in archive and destination blob did
           not rehydrate yet before being overwritten.

        Parameters
        ----------
        src_path: pathlib.Path
            Path to the source blob
        dst_path: pathlib.Path
            Path to destination blob
        dst_container_name: string
            Name of destination container. If none, source container is also destination container
        dst_blob_tier: azure.storage.blob.StandardBlobTier
            Tier of the destination blob
        rehydrate_priority: from azure.storage.blob.RehydratePriority
            Rehydrate priority in case source blob is in archive tier
        kwargs: dict
            Additional parameters to pass to BlobClient.start_copy_from_url
            https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobclient?view=azure-python#azure-storage-blob-blobclient-start-copy-from-url
        """

        if dst_container_name is None:
            dst_container = self.container
        else:
            dst_container = self.block_blob_service.get_container_client(
                dst_container_name
            )

        src_blob = self.container.get_blob_client(str(src_path))
        dst_blob = dst_container.get_blob_client(str(dst_path))
        dst_blob.start_copy_from_url(
            src_blob.url,
            standard_blob_tier=dst_blob_tier,
            rehydrate_priority=rehydrate_priority,
            **kwargs,
        )
