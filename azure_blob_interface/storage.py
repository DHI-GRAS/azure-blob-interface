import abc
from typing import List


class StorageDriver(metaclass=abc.ABCMeta):
    """All implementations of download, upload and delete should by defult be recursive"""

    @abc.abstractmethod
    def download() -> None:
        pass

    @abc.abstractmethod
    def upload() -> None:
        pass

    @abc.abstractmethod
    def delete() -> None:
        pass

    @abc.abstractmethod
    def exists() -> bool:
        pass

    @abc.abstractmethod
    def list_files() -> List[str]:
        pass

    @abc.abstractmethod
    def rename() -> bool:
        pass
