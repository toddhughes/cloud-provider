import unittest

from src.cloud import CloudProvider

cloud = CloudProvider()


class TestContainer(unittest.TestCase):
    def test_get_object(
            self):
        container_name = 'test-1'
        object_key = 'dir1/20251006_091147.jpg'

        my_object = cloud.storage.get_object(container_name, object_key)
        self.assertTrue(my_object, 'The object should not be empty.')

    def test_list_objects(
            self):
        container_name = 'test-1'
        prefix = 'dir1/2025'

        my_list = cloud.storage.list_objects(container_name, prefix)
        self.assertTrue(my_list, 'The list should not be empty.')
