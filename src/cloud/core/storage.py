from abc import ABC, abstractmethod
from io import BytesIO
from logging import Logger
from typing import Union


class Storage(ABC):
    def __init__(
            self,
            logger: Union[Logger, None] = None,
            **kwargs):
        self._logger = logger
        self._kwargs = kwargs

    @abstractmethod
    def copy_object(
            self,
            source_container: str,
            source_key: str,
            target_container: str,
            target_key: str,
            **kwargs):
        pass

    @abstractmethod
    def delete_object(
            self,
            container_name: str,
            object_key: str,
            **kwargs):
        pass

    @abstractmethod
    def delete_objects(
            self,
            container_name: str,
            object_keys: list[str],
            **kwargs):
        pass

    @abstractmethod
    def download_object(
            self,
            container_name: str,
            object_key: str,
            download_path: str,
            **kwargs):
        pass

    @abstractmethod
    def get_object(
            self,
            container_name: str,
            object_key: str,
            **kwargs) -> Union[BytesIO, None]:
        pass

    @abstractmethod
    def list_objects(
            self,
            container_name: str,
            prefix: str,
            **kwargs):
        pass

    @abstractmethod
    def objects_exist(
            self,
            container_name: str,
            prefix: str,
            **kwargs) -> bool:
        pass
