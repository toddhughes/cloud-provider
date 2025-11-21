from logging import Logger
from typing import Union

from .azure.azure_provider import AzureProvider


class ProviderFactory:
    @staticmethod
    def get_provider(
            provider_type: str,
            logger: Union[Logger, None] = None,
            **kwargs):

        if provider_type.lower() == 'azure':
            return AzureProvider(logger, **kwargs)
        else:
            raise ValueError(f"Unknown provider type: {provider_type}")
