import pandas as pd
import requests

from bs4 import BeautifulSoup


def get_bike_stations_data():
    url = 'https://wroclawskirower.pl/en/stations-map/'

    page = requests.get(url)
    soup = BeautifulSoup(page.text, features="html.parser")

    data = []
    table = soup.find(name='div', class_='station_list').find(name='table')
    rows = table.find_all(name='tr')

    # header
    header_row = rows[0]
    header_cols = [ele.text.strip() for ele in header_row.find_all('th')]

    # data
    for row in rows[1:]:  
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        data.append(cols)

    return pd.DataFrame(data=data, columns=header_cols)