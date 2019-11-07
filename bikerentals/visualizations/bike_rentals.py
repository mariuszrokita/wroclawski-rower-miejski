import numpy as np
import os
import pandas as pd
import streamlit as st


data_filepath = os.path.join(os.getcwd(), '..', 'data', 'processed', 'bike_rentals.csv')


@st.cache
def fetch_and_clean_data():
    data = pd.read_csv(data_filepath)
    idx = data[data['IsDeleted'] == False].index  # noqa E712
    data_subset = data.loc[idx, ['Rental station latitude', 'Rental station longitude', 'Rental hour']]
    data_subset.columns = ['latitude', 'longitude', 'hour']
    return data_subset


st.title('Bike rentals')

hour = st.slider("Hour to look at", 0, 23)

data = fetch_and_clean_data()
filtered_data = data[data['hour'] == hour]

st.subheader("Bike rentals between %i:00 and %i:00" % (hour, (hour + 1) % 24))
midpoint = (np.average(filtered_data['latitude']), np.average(filtered_data['longitude']))
st.deck_gl_chart(
    viewport={
        "latitude": midpoint[0],
        "longitude": midpoint[1],
        "zoom": 11,
        "pitch": 50,
    },
    layers=[
        {
            "type": "HexagonLayer",
            "data": filtered_data,
            "radius": 100,
            "elevationScale": 4,
            "elevationRange": [0, 1000],
            "pickable": True,
            "extruded": True,
        }
    ],
)

#st.map(filtered_data)

if st.checkbox("Show raw data", False):
    st.subheader("Raw data by minute between %i:00 and %i:00" % (hour, (hour + 1) % 24))
    st.write(filtered_data)
