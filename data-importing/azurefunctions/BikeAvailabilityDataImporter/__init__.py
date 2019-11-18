import datetime
import logging
import os

import azure.functions as func

from .bike_availability_data_import import import_bike_availability_data


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    try:
        # configuration
        bike_availability_data_url = os.environ['bike_availability_data_url']
        storage_account_name = os.environ['storage_account_name']
        storage_account_key = os.environ['storage_account_key']
        storage_container_name = os.environ['bike_availability_container_name']

        # run import
        import_bike_availability_data(bike_availability_data_url,
                                      storage_account_name,
                                      storage_account_key,
                                      storage_container_name)

    except Exception as e:
        logging.error('Error:')
        logging.error(e, exc_info=True)
