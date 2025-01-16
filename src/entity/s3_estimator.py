from azure_storage import AzureBlobStorageService
from src.exception import MyException
from src.entity.estimator import MyModel
import sys
from pandas import DataFrame
import os


class Proj1EstimatorAzure:
    """
    This class is used to save and retrieve our model from Azure Blob Storage and to do predictions.
    """

    def __init__(self, connection_string: str, container_name: str, model_path: str):
        """
        :param connection_string: Azure Blob Storage connection string.
        :param container_name: Name of your container in Azure Blob Storage.
        :param model_path: Location of your model blob in the container.
        """
        self.connection_string = connection_string
        self.container_name = container_name
        self.model_path = model_path
        self.azure_storage = AzureBlobStorageService(connection_string)
        self.loaded_model: MyModel = None

    def is_model_present(self) -> bool:
        """
        Check if the model blob exists in the Azure container.
        :return: True if the blob exists, False otherwise.
        """
        try:
            return self.azure_storage.blob_exists(self.container_name, self.model_path)
        except MyException as e:
            print(e)
            return False

    def load_model(self) -> MyModel:
        """
        Load the model from the Azure Blob Storage.
        :return: The loaded MyModel object.
        """
        try:
            temp_file_path = "temp_model.pkl"
            self.azure_storage.download_blob(self.container_name, self.model_path, temp_file_path)
            with open(temp_file_path, "rb") as model_file:
                model = pickle.load(model_file)
            os.remove(temp_file_path)
            return model
        except Exception as e:
            raise MyException(e, sys)

    def save_model(self, from_file: str, remove: bool = False) -> None:
        """
        Save the model to the Azure Blob Storage.
        :param from_file: Path to the local model file.
        :param remove: If True, remove the local file after upload.
        """
        try:
            self.azure_storage.upload_file(from_file, self.container_name, self.model_path)
            if remove:
                os.remove(from_file)
        except Exception as e:
            raise MyException(e, sys)

    def predict(self, dataframe: DataFrame):
        """
        Make predictions using the loaded model.
        :param dataframe: Input DataFrame for predictions.
        :return: Predictions from the model.
        """
        try:
            if self.loaded_model is None:
                self.loaded_model = self.load_model()
            return self.loaded_model.predict(dataframe=dataframe)
        except Exception as e:
            raise MyException(e, sys)
