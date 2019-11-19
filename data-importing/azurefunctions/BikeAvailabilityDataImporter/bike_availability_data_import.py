"""
This module contains functions to import bike availability data
and to persist them in Azure Blob Storage.
"""

import json
import logging
import requests

from datetime import datetime

from azure.storage.blob import BlockBlobService


def get_bike_availability_data(url: str) -> str:
    """Get bike availability data that is available at the provided address.

    Arguments:
        url {str} -- The URL address where we can find information about available bikes.

    Returns:
        str -- Data in a form of a JSON document.
    """
    r = requests.get(url)
    # change the encoding for the request content
    r.encoding = 'utf-8'
    return r.text


def import_bike_availability_data(url: str, account_name: str, account_key: str, container_name: str) -> None:
    """Import bike data availability and save it into Azure Blob Storage container.

    Arguments:
        url {str} -- The URL address where we can find information about available bikes.
        account_name {str} -- The storage account name.
        account_key {str} -- The storage account key.
        container_name {str} -- Name of existing container.
    """
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
