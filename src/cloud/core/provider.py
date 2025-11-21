from abc import ABC, abstractmethod
from logging import Logger
from typing import Union

from .storage import Storage


class Provider(ABC):
    def __init__(
            self,
            logger: Union[Logger, None] = None,
            **kwargs):
        self._logger = logger
        self._kwargs = kwargs

    @property
    @abstractmethod
    def storage(
            self) -> Storage:
        pass
