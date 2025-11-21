import os
from logging import getLogger, INFO, Logger
from typing import Union

from .core.storage import Storage
from .provider_factory import ProviderFactory


class CloudProvider:
    def __init__(
            self,
            provider_type: str = 'Azure',
            logger: Union[Logger, None] = None,
            **kwargs):

        if provider_type is None:
            provider_type = os.getenv('CLOUD_PROVIDER', 'Azure')

        if logger is None:
            logger = getLogger('cloud_provider')
            logger.setLevel(INFO)

        self._provider = ProviderFactory.get_provider(provider_type, logger, **kwargs)

    def __repr__(
            self):
        return f'CloudProvider(provider_type={self._provider})'

    @property
    def storage(
            self) -> Storage:
        return self._provider.storage
