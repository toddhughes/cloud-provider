from io import BytesIO
from logging import Logger
from typing import cast, Literal, Sequence, Union

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

    def download_object(
            self,
            container_name: str,
            object_key: str,
            download_path: str,
            **kwargs):
        """
        Download a blob from Azure Storage to a local file path.

        Args:
            container_name: Name of the container
            object_key: The blob name/key to download
            download_path: Local file path where the blob will be saved
            **kwargs: Additional arguments (e.g., max_concurrency, timeout, version_id)

        Returns:
            None
        """
        from azure.core.exceptions import ResourceNotFoundError
        import os

        try:
            # Create parent directories if they don't exist.
            os.makedirs(os.path.dirname(download_path), exist_ok=True)

            blob_client = self._blob_client.get_blob_client(container=container_name, blob=object_key)

            with open(download_path, 'wb') as download_file:
                download_file.write(cast(bytes, blob_client.download_blob(**kwargs).readall()))

        except ResourceNotFoundError:
            raise FileNotFoundError(f"Blob not found: {container_name}/{object_key}")

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

    def objects_exist(
            self,
            container_name: str,
            prefix: str,
            **kwargs) -> bool:
        """
        Check if one or more objects exist in the container with the supplied prefix.

        Args:
            container_name: Name of the container
            prefix: Prefix to filter blob names
            **kwargs: Additional arguments (e.g., timeout)

        Returns:
            True if at least one object exists with the prefix, False otherwise
        """
        try:
            container_client = self._blob_client.get_container_client(container_name)

            # Use results_per_page=1 for efficiency since we only need to know if any exist.
            blob_list = container_client.list_blobs(
                name_starts_with=prefix,
                results_per_page=1,
                **kwargs
            )

            # Check if there's at least one blob.
            for _ in blob_list:
                return True

            return False

        except Exception:  # noqa
            return False

    def read_into_df(
            self,
            container_name: str,
            object_key: str,
            separator: Union[str, None],
            header: Union[int, Sequence[int], Literal['infer', None]],
            na_values: Union[str, int, None],
            **kwargs):
        """
        Read a blob from Azure Storage into a pandas DataFrame.

        Args:
            container_name: Name of the container
            object_key: The blob name/key to read
            separator: Delimiter to use (e.g., ',' for CSV, '\t' for TSV, None for whitespace)
            header: Row number(s) to use as column names, 'infer' to auto-detect, or None for no header
            na_values: Additional strings to recognize as NA/NaN
            **kwargs: Additional arguments to pass to pandas.read_csv

        Returns:
            pandas DataFrame, or None if blob not found
        """
        import pandas as pd

        # Get the object as BytesIO.
        blob_data = self.get_object(container_name, object_key, **kwargs)

        if blob_data is None:
            return None

        # Read into DataFrame.
        df = pd.read_csv(
            blob_data,
            sep=separator,
            header=header,
            na_values=na_values,
            **kwargs
        )

        return df

    def upload_object(
            self,
            file: Union[str, BytesIO],
            container_name: str,
            object_key: str,
            **kwargs):
        """
        Upload a file to Azure Blob Storage from a local file path or BytesIO object.

        Args:
            file: Local file path (str) or BytesIO object to upload
            container_name: Name of the container
            object_key: The blob name/key to create
            **kwargs: Additional arguments (e.g., overwrite, metadata, content_settings, max_concurrency)

        Returns:
            None
        """
        blob_client = self._blob_client.get_blob_client(container=container_name, blob=object_key)

        if isinstance(file, str):
            # Upload from a file path.
            with open(file, "rb") as data:
                blob_client.upload_blob(data, **kwargs)
        elif isinstance(file, BytesIO):
            # Upload from BytesIO object.
            file.seek(0)  # Reset to the beginning of BytesIO.
            blob_client.upload_blob(file, **kwargs)
        else:
            raise TypeError("File must be either a string (file path) or BytesIO object")
