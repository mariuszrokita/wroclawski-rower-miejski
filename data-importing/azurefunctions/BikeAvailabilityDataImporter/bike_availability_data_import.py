"""
This module contains functions to import bike availability data
and to persist them in Azure Blob Storage.
"""

import json
import logging
import requests

from datetime import datetime

from azure.storage.blob import BlockBlobService


def get_bike_availability_data(url):
    r = requests.get(url)
    # change the encoding for the request content
    r.encoding = 'utf-8'
    return r.text


def import_bike_availability_data(url, account_name, account_key, container_name):
    block_blob_service = BlockBlobService(account_name, account_key)

    now = datetime.now()

    # HACK: compare 'offset' and 'total' values returned from API to determine
    for offset in [0, 100, 200]:
        url_with_offset = f'{url}&offset={offset}'
        data = get_bike_availability_data(url_with_offset)
        json_data = json.loads(data)
        json_data['datetime'] = now.isoformat()

        filename = f'{now.strftime("%Y_%m_%d_%H_%M_%S")}_{offset}.json'
        block_blob_service.create_blob_from_text(container_name, filename, json.dumps(json_data))
        logging.info(f"Created blob file '{filename}' in the container '{container_name}'")
