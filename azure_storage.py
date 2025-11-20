import os
from io import BytesIO


class AzureStorage:
    def __init__(
            self):
        from azure.storage.blob import BlobServiceClient

        def get_service_client(
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

        self._blob_client = get_service_client(os.environ.get('AZURE_ACCOUNT_NAME'))

    def get_object(
            self,
            container_name: str,
            object_key: str) -> BytesIO:
        """
        Get blob content by key as BytesIO object.

        Args:
            container_name: Name of the container
            object_key: The blob name/key

        Returns:
            BytesIO object containing blob content
        """

        blob_client = self._blob_client.get_blob_client(container=container_name, blob=object_key)
        blob_data = blob_client.download_blob().readall()

        return BytesIO(blob_data)

    def list_objects(
            self,
            container_name: str,
            prefix: str):
        """

        :param container_name:
        :param prefix:
        :return:
        """

        container_client = self._blob_client.get_container_client(container_name)
        blob_list = container_client.list_blobs(name_starts_with=prefix)

        object_keys = []

        for blob in blob_list:
            object_keys.append(blob.name)

        return object_keys


ac = AzureStorage()
obj_list = ac.list_objects('test-1', 'dir1/2025')

b = ac.get_object('test-1', obj_list[0])
print(b)
