import unittest

from src.cloud.azure.azure_storage import AzureStorage


class TestContainer(unittest.TestCase):
    def test_get_object(
            self):
        ac = AzureStorage()

        container_name = 'test-1'
        object_key = 'dir1/20251006_091147.jpg'

        my_object = ac.get_object(container_name, object_key)
        self.assertTrue(my_object, 'The object should not be empty.')

    def test_list_objects(
            self):
        container_name = 'test-1'
        prefix = 'dir1/2025'

        ac = AzureStorage()
        my_list = ac.list_objects(container_name, prefix)
        self.assertTrue(my_list, 'The list should not be empty.')
