import unittest

from src.cloud import CloudProvider

cloud = CloudProvider()


class TestContainer(unittest.TestCase):
    def test_copy_object(
            self):
        source_container = 'test-1'
        source_key = 'dir1/20251006_091147.jpg'

        target_container = 'test-2'
        target_key = 'dir3/20251006_091147.jpg'

        cloud.storage.copy_object(source_container, source_key, target_container, target_key)

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
