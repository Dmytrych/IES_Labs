import json
import logging
from typing import List

import pydantic_core
import requests

from datetime import datetime
from app.entities.processed_agent_data import ProcessedAgentData
from app.interfaces.store_gateway import StoreGateway

class StoreApiAdapter(StoreGateway):
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url

    def save_data(self, processed_agent_data_batch: List[ProcessedAgentData]):
        """
        Save the processed road data to the Store API.
        Parameters:
            processed_agent_data_batch (dict): Processed road data to be saved.
        Returns:
            bool: True if the data is successfully saved, False otherwise.
        """
        print("Hub: sending processed data")

        post_url = f"{self.api_base_url}/processed_agent_data/"
        json_data_array = [agent_data.model_dump_json() for agent_data in processed_agent_data_batch]

        try:
            response = requests.post(post_url, data='[' + ','.join(json_data_array) + ']',
                                     headers={'Content-Type': 'application/json'})

            if 200 > response.status_code >= 300:
                return False

            print("Hub: sending processed data was completed successfully")
            return True
        except:
            print("Hub: an error occurred during data saving")
            return False





