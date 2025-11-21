from logging import Logger
from typing import Union

from .azure_storage import AzureStorage
from ..core.provider import Provider
from ..core.storage import Storage


class AzureProvider(Provider):
    def __init__(
            self,
            logger: Union[Logger, None] = None,
            **kwargs):
        super().__init__(logger, **kwargs)

    @property
    def storage(
            self) -> Storage:
        return AzureStorage(self._logger, **self._kwargs)
