"""
This module contains functions to import csv files with historic records
and to persist them in Azure Blob Storage.
"""

import logging
import os
import requests

from azure.storage.blob import BlockBlobService
from bs4 import BeautifulSoup


def get_historic_csv_file_links_to_download(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.text, features="html.parser")

    csv_anchors = soup.find_all(name="a", class_="resource-url-analytics")
    return [link['href']
            for link in csv_anchors
            if "Pobierz" in link.text]


def get_blob_names_already_imported(block_blob_service, container_name):
    generator = block_blob_service.list_blobs(container_name)
    return [blob.name for blob in generator]


def import_historic_csv_files(url, account_name, account_key, container_name):
    block_blob_service = BlockBlobService(account_name, account_key)
    blobs_already_imported = get_blob_names_already_imported(block_blob_service, container_name)

    counter = 0
    historic_csv_links = get_historic_csv_file_links_to_download(url)
    for csv_link in historic_csv_links:
        filename = os.path.basename(csv_link)
        if filename not in blobs_already_imported:
            counter += 1
            # import csv file - read file's content and upload it to Azure Blob Storage
            r = requests.get(csv_link)
            # change the encoding for the request content
            r.encoding = 'utf-8'
            block_blob_service.create_blob_from_text(container_name, filename, r.text)
            logging.info(f"Created blob file '{filename}' in the container '{container_name}'")

    if counter == 0:
        logging.info(f"Nothing new to import. All CSV files are already uploaded to Blob Storage.")
