from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from io import StringIO
from typing import Union, List
import os
import logging
import pickle
import pandas as pd


class AzureBlobStorageService:
    """
    A class for interacting with Azure Blob Storage, providing methods for file management,
    data uploads, and data retrieval in Azure containers.
    """

    def __init__(self, connection_string: str = None):
        """
        Initializes the AzureBlobStorageService instance with BlobServiceClient.

        Args:
            connection_string (str): The Azure Storage connection string. If not provided,
                                     it will use the AZURE_STORAGE_CONNECTION_STRING environment variable.
        """
        if not connection_string:
            connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        if not connection_string:
            raise ValueError("Azure Storage connection string is required.")

        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    def container_exists(self, container_name: str) -> bool:
        """
        Checks if the specified container exists.

        Args:
            container_name (str): Name of the Azure Blob Storage container.

        Returns:
            bool: True if the container exists, False otherwise.
        """
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            return container_client.exists()
        except Exception as e:
            logging.error(f"Error checking if container exists: {e}")
            raise

    def blob_exists(self, container_name: str, blob_name: str) -> bool:
        """
        Checks if a specified blob exists in the container.

        Args:
            container_name (str): Name of the container.
            blob_name (str): Name of the blob.

        Returns:
            bool: True if the blob exists, False otherwise.
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(container_name, blob_name)
            return blob_client.exists()
        except Exception as e:
            logging.error(f"Error checking if blob exists: {e}")
            raise

    def upload_file(self, file_path: str, container_name: str, blob_name: str):
        """
        Uploads a local file to the specified container.

        Args:
            file_path (str): Path of the local file.
            container_name (str): Name of the container.
            blob_name (str): Name of the blob in the container.
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(container_name, blob_name)
            with open(file_path, "rb") as file:
                blob_client.upload_blob(file, overwrite=True)
            logging.info(f"Uploaded {file_path} to {container_name}/{blob_name}")
        except Exception as e:
            logging.error(f"Error uploading file: {e}")
            raise

    def download_blob(self, container_name: str, blob_name: str, file_path: str):
        """
        Downloads a blob from the container to a local file.

        Args:
            container_name (str): Name of the container.
            blob_name (str): Name of the blob.
            file_path (str): Local path to save the downloaded file.
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(container_name, blob_name)
            with open(file_path, "wb") as file:
                download_stream = blob_client.download_blob()
                file.write(download_stream.readall())
            logging.info(f"Downloaded {container_name}/{blob_name} to {file_path}")
        except Exception as e:
            logging.error(f"Error downloading blob: {e}")
            raise

    def upload_dataframe_as_csv(self, df: pd.DataFrame, container_name: str, blob_name: str):
        """
        Uploads a DataFrame as a CSV file to the specified container.

        Args:
            df (pd.DataFrame): DataFrame to upload.
            container_name (str): Name of the container.
            blob_name (str): Name of the blob in the container.
        """
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            blob_client = self.blob_service_client.get_blob_client(container_name, blob_name)
            blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)
            logging.info(f"Uploaded DataFrame as CSV to {container_name}/{blob_name}")
        except Exception as e:
            logging.error(f"Error uploading DataFrame as CSV: {e}")
            raise

    def read_blob_as_dataframe(self, container_name: str, blob_name: str) -> pd.DataFrame:
        """
        Reads a blob as a DataFrame.

        Args:
            container_name (str): Name of the container.
            blob_name (str): Name of the blob.

        Returns:
            pd.DataFrame: DataFrame created from the blob content.
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(container_name, blob_name)
            download_stream = blob_client.download_blob()
            csv_content = download_stream.readall().decode("utf-8")
            df = pd.read_csv(StringIO(csv_content))
            logging.info(f"Read blob {container_name}/{blob_name} into DataFrame")
            return df
        except Exception as e:
            logging.error(f"Error reading blob as DataFrame: {e}")
            raise

    def create_container(self, container_name: str):
        """
        Creates a container if it doesn't exist.

        Args:
            container_name (str): Name of the container.
        """
        try:
            container_client = self.blob_service_client.get_container_client(container_name)
            if not container_client.exists():
                container_client.create_container()
                logging.info(f"Created container: {container_name}")
            else:
                logging.info(f"Container {container_name} already exists.")
        except Exception as e:
            logging.error(f"Error creating container: {e}")
            raise

    def delete_blob(self, container_name: str, blob_name: str):
        """
        Deletes a blob from the container.

        Args:
            container_name (str): Name of the container.
            blob_name (str): Name of the blob to delete.
        """
        try:
            blob_client = self.blob_service_client.get_blob_client(container_name, blob_name)
            blob_client.delete_blob()
            logging.info(f"Deleted blob: {container_name}/{blob_name}")
        except Exception as e:
            logging.error(f"Error deleting blob: {e}")
            raise
