from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure_credentials import credentials
import os


class AzureBlobClient:
    blob_service_client = None

    def __init__(self):
        """
        This class retrieves the Azure connection string from an environment variable,
        creates a connection with Azure Blob Storage, and raises an exception
        if the environment variable is not set.
        """

        if AzureBlobClient.blob_service_client is None:
            # Retrieve the Azure connection string from the environment variable
            connection_string_env_key = credentials.AZURE_STORAGE_CONNECTION_STRING
            connection_string = os.getenv(connection_string_env_key)

            if connection_string is None:
                raise Exception(f"Environment variable: {connection_string_env_key} is not set.")

            # Initialize the BlobServiceClient
            AzureBlobClient.blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        self.blob_service_client = AzureBlobClient.blob_service_client
