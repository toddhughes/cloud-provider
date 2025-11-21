from io import BytesIO
from logging import Logger
from typing import Union

from ..core.storage import Storage


class AzureStorage(Storage):
    def __init__(
            self,
            logger: Union[Logger, None] = None,
            **kwargs):
        super().__init__(logger, **kwargs)

        from azure.storage.blob import BlobServiceClient

        def _get_service_client(
                account_name: str) -> BlobServiceClient:
            """
            Create a BlobServiceClient using DefaultAzureCredential.
            Similar to how boto3 automatically finds credentials.

            Args:
                account_name: Your Azure Storage account name

            Returns:
                BlobServiceClient
            """
            from azure.identity import DefaultAzureCredential

            account_url = f"https://{account_name}.blob.core.windows.net"
            credential = DefaultAzureCredential()

            return BlobServiceClient(account_url, credential=credential)  # noqa

        from dotenv import load_dotenv
        import os

        # Load environment variables from the .env file.
        load_dotenv()

        # Access environment variables
        account = os.getenv("AZURE_ACCOUNT_NAME")

        self._blob_client = _get_service_client(account)

    def copy_object(
            self,
            source_container: str,
            source_key: str,
            destination_container: str,
            destination_key: str,
            **kwargs):
        def copy_object(
                self,
                source_container: str,
                source_key: str,
                destination_container: str,
                destination_key: str,
                **kwargs):
            """
            Copy a blob from source to destination within Azure Blob Storage.

            Args:
                source_container: Source container name
                source_key: Source blob name/key
                destination_container: Destination container name
                destination_key: Destination blob name/key
                **kwargs: Additional arguments (e.g., metadata, timeout)

            Returns:
                Copy operation properties
            """
            # Get the source blob client to construct the source URL
            source_blob_client = self._blob_client.get_blob_client(
                container=source_container,
                blob=source_key
            )

            # Get the destination blob client
            dest_blob_client = self._blob_client.get_blob_client(
                container=destination_container,
                blob=destination_key
            )

            # Start the copy operation using the source blob URL
            copy_operation = dest_blob_client.start_copy_from_url(
                source_blob_client.url,
                **kwargs
            )

            return copy_operation

    def get_object(
            self,
            container_name: str,
            object_key: str,
            **kwargs) -> BytesIO:
        blob_client = self._blob_client.get_blob_client(container=container_name, blob=object_key)
        blob_data = blob_client.download_blob().readall()

        return BytesIO(blob_data)

    def list_objects(
            self,
            container_name: str,
            prefix: str,
            **kwargs):
        container_client = self._blob_client.get_container_client(container_name)
        blob_list = container_client.list_blobs(name_starts_with=prefix)

        object_keys = []

        for blob in blob_list:
            object_keys.append(blob.name)

        return object_keys
