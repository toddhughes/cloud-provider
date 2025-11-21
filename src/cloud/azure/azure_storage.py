from io import BytesIO
from logging import Logger
from typing import cast, Union

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

            return BlobServiceClient(account_url, credential=credential, **self._kwargs)  # noqa

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
            target_container: str,
            target_key: str,
            **kwargs):
        """
        Copy a blob from source to destination within Azure Blob Storage.

        Args:
            source_container: Source container name
            source_key: Source blob name/key
            target_container: Destination container name
            target_key: Destination blob name/key
            **kwargs: Additional arguments (e.g., metadata, timeout)

        Returns:
            Copy operation properties
        """
        # Get the source blob client to construct the source URL
        source_blob_client = self._blob_client.get_blob_client(
            container=source_container,
            blob=source_key
        )

        # Get the destination blob client.
        dest_blob_client = self._blob_client.get_blob_client(
            container=target_container,
            blob=target_key
        )

        # Start the copy operation using the source blob URL.
        copy_operation = dest_blob_client.start_copy_from_url(
            source_blob_client.url,
            **kwargs
        )

        return copy_operation

    def delete_object(
            self,
            container_name: str,
            object_key: str,
            **kwargs):
        """
        Delete a blob from the specified container.

        Args:
            container_name: Name of the container
            object_key: The blob name/key to delete
            **kwargs: Additional arguments (e.g., delete_snapshots, timeout, lease)

        Returns:
            None
        """
        blob_client = self._blob_client.get_blob_client(container=container_name, blob=object_key)
        blob_client.delete_blob(**kwargs)

    def delete_objects(
            self,
            container_name: str,
            object_keys: list[str],
            **kwargs):

        for object_key in object_keys:
            blob_client = self._blob_client.get_blob_client(container=container_name, blob=object_key)
            blob_client.delete_blob(**kwargs)

    def get_object(
            self,
            container_name: str,
            object_key: str,
            **kwargs) -> Union[BytesIO, None]:
        """
        Get blob content by key as BytesIO object.

        Args:
            container_name: Name of the container
            object_key: The blob name/key
            **kwargs: Additional arguments (e.g., max_concurrency, timeout, version_id)

        Returns:
            BytesIO object containing blob content, or None if blob not found
        """
        from azure.core.exceptions import ResourceNotFoundError

        try:
            blob_client = self._blob_client.get_blob_client(container=container_name, blob=object_key)
            blob_data = cast(bytes, blob_client.download_blob(**kwargs).readall())

            return BytesIO(blob_data)
        except ResourceNotFoundError:
            return None

    def list_objects(
            self,
            container_name: str,
            prefix: str,
            **kwargs):
        """
        List blobs in a container with optional prefix filter.

        Args:
            container_name: Name of the container
            prefix: Prefix to filter blob names
            **kwargs: Additional arguments (e.g., include, timeout, results_per_page)

        Returns:
            List of blob names/keys
        """
        container_client = self._blob_client.get_container_client(container_name)
        blob_list = container_client.list_blobs(name_starts_with=prefix, **kwargs)

        object_keys = []

        for blob in blob_list:
            object_keys.append(blob.name)

        return object_keys
