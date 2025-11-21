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

    def test_delete_object(
            self):
        source_container = 'test-1'
        source_key = 'dir1/20251006_091147.jpg'

        target_container = 'test-2'
        target_key = 'dir4/20251006_091147.jpg'

        # Add a new object.
        cloud.storage.copy_object(source_container, source_key, target_container, target_key)

        # Ensure it was copied.
        my_object = cloud.storage.get_object(target_container, target_key)
        self.assertTrue(my_object, 'The object should not be empty.')

        # Delete it.
        cloud.storage.delete_object(target_container, target_key)

        # Ensure it was deleted.
        my_object = cloud.storage.get_object(target_container, target_key)
        self.assertFalse(my_object, 'The object should be empty.')

    def test_download_object(
            self):
        import os
        import tempfile

        container_name = 'test-1'
        object_key = 'dir1/20251006_091147.jpg'

        # Create a temporary file path.
        with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as tmp_file:
            download_path = tmp_file.name

        try:
            # Download the object.
            cloud.storage.download_object(container_name, object_key, download_path)

            # Assert the file was created and has content
            self.assertTrue(os.path.exists(download_path), 'The downloaded file should exist.')
            self.assertGreater(os.path.getsize(download_path), 0, 'The downloaded file should not be empty.')

        finally:
            # Clean up.
            if os.path.exists(download_path):
                os.remove(download_path)

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

    def test_objects_exist(
            self):
        container_name = 'test-1'
        prefix = 'dir1/'

        # Test with existing prefix.
        exists = cloud.storage.objects_exist(container_name, prefix)
        self.assertTrue(exists, 'Objects should exist with the given prefix.')

        # Test with non-existing prefix.
        non_existing_prefix = 'nonexistent/path/that/does/not/exist/'
        not_exists = cloud.storage.objects_exist(container_name, non_existing_prefix)
        self.assertFalse(not_exists, 'Objects should not exist with the non-existing prefix.')

    def test_read_into_df(
            self):
        import pandas as pd
        from io import BytesIO

        container_name = 'test-1'
        object_key = 'data/test_data.csv'

        # Create and upload test CSV data.
        test_csv = "col1,col2,col3\n1,2,3\n4,5,6\n7,8,9"
        csv_data = BytesIO(test_csv.encode('utf-8'))

        # Upload the test data.
        cloud.storage.upload_object(csv_data, container_name, object_key, overwrite=True)

        try:
            # Read into DataFrame.
            df = cloud.storage.read_into_df(
                container_name,
                object_key,
                separator=',',
                header='infer',
                na_values=None
            )

            # Assertions.
            self.assertIsNotNone(df, 'The DataFrame should not be None.')
            self.assertIsInstance(df, pd.DataFrame, 'The result should be a pandas DataFrame.')
            self.assertEqual(len(df), 3, 'The DataFrame should have 3 rows.')
            self.assertEqual(len(df.columns), 3, 'The DataFrame should have 3 columns.')
            self.assertListEqual(list(df.columns), ['col1', 'col2', 'col3'], 'Column names should match.')

            # Test non-existent file.
            df_none = cloud.storage.read_into_df(
                container_name,
                'nonexistent.csv',
                separator=',',
                header='infer',
                na_values=None
            )
            self.assertIsNone(df_none, 'Should return None for non-existent file.')

        finally:
            # Clean up.
            cloud.storage.delete_object(container_name, object_key)

    def test_upload_object(
            self):
        import os
        import tempfile

        container_name = 'test-1'
        object_key = 'dir1/test_upload.txt'
        test_content = b'This is a test file for upload.'

        # Test 1: Upload from a file path.
        with tempfile.NamedTemporaryFile(delete=False, mode='wb') as tmp_file:
            tmp_file.write(test_content)
            tmp_path = tmp_file.name

        try:
            # Upload the file.
            cloud.storage.upload_object(tmp_path, container_name, object_key, overwrite=True)

            # Verify it was uploaded by downloading it back
            downloaded = cloud.storage.get_object(container_name, object_key)
            self.assertIsNotNone(downloaded, 'The uploaded object should exist.')
            self.assertEqual(downloaded.read(), test_content, 'The uploaded content should match.')

        finally:
            # Clean up the local file.
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

            # Clean up uploaded blob.
            cloud.storage.delete_object(container_name, object_key)

        # Test 2: Upload from BytesIO.
        from io import BytesIO
        bytes_data = BytesIO(test_content)

        cloud.storage.upload_object(bytes_data, container_name, object_key, overwrite=True)

        # Verify it was uploaded.
        downloaded = cloud.storage.get_object(container_name, object_key)
        self.assertIsNotNone(downloaded, 'The uploaded object from BytesIO should exist.')
        self.assertEqual(downloaded.read(), test_content, 'The uploaded content from BytesIO should match.')

        # Clean up.
        cloud.storage.delete_object(container_name, object_key)
