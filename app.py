import math
import numpy as np
import pandas as pd
import streamlit as st


def filter_on_change():
    df = st.session_state['data']

    if st.session_state['selected_routes']:
        df = df.loc[df['RouteShortName'].isin(
            st.session_state['selected_routes'])]

    if st.session_state['trip_direction']:
        df = df.loc[df['TripHeadsign'] == st.session_state['trip_direction']]

    if st.session_state['departure_stop'] and st.session_state['departure_time']:
        routes = df.loc[
            (df['StopName'] == st.session_state['departure_stop']) &
            (df['DepartureTime'] == str(st.session_state['departure_time'])) &
            (df['TripHeadsign'] == st.session_state['trip_direction'])
        ]

        if routes.shape[0] > 0:
            active_route = routes['TripID'].values[0]

            st.session_state['active_route'] = active_route

            df = df.loc[df['TripID'] == active_route].sort_values(
                by="StopSequence")

        else:
            st.session_state['active_route'] = None

    else:
        st.session_state['active_route'] = None

    st.session_state['filtered_data'] = df


@st.cache_data(ttl=90)
def load_data():
    print("Reloading cache")
    data = pd.read_parquet(
        "https://fsn1.your-objectstorage.com/gtfs-public-data/new_gtfsfeed.parquet")
    return data


st.session_state['data'] = load_data()


if 'available_routes' not in st.session_state:
    st.session_state['available_routes'] = st.session_state['data']['RouteShortName'].unique()

if 'trip_direction' not in st.session_state:
    st.session_state['trip_direction'] = None


# Data container
with st.container(border=True, key="search_form"):
    st.multiselect(label="Select Routes:",
                   key='selected_routes',
                   options=st.session_state['available_routes'],
                   default=['IR75'])

    available_directions = st.session_state['data'].loc[st.session_state['data']['RouteShortName'].isin(
        st.session_state['selected_routes'])]['TripHeadsign'].unique()

    st.pills(
        label="Direction:",
        key='trip_direction',
        options=available_directions,
    )

    available_departure_stops = st.session_state['data'].loc[(st.session_state['data']['RouteShortName'].isin(
        st.session_state['selected_routes'])) & (st.session_state['data']['TripHeadsign'] == st.session_state['trip_direction'])]['StopName'].unique()

    st.pills(
        label="Departure Stop:",
        key="departure_stop",
        options=available_departure_stops
    )

    st.time_input(
        label="Departure at station:",
        key="departure_time",
        value=None
    )


# Init filter data
filter_on_change()


if st.session_state['active_route']:
    stops_list = st.session_state['filtered_data'].sort_values(by='StopSequence')[
        'StopName'].to_list()

    start_stop, end_stop = st.select_slider(
        "Select start and end station",
        options=stops_list,
        value=(st.session_state['departure_stop'], stops_list[-1])
    )

    start_stop_sequence = st.session_state['filtered_data'].loc[st.session_state['filtered_data']['StopName']
                                                                == start_stop]['StopSequence'].values[0]

    end_stop_sequence = st.session_state['filtered_data'].loc[st.session_state['filtered_data']['StopName']
                                                              == end_stop]['StopSequence'].values[0]

    st.session_state['filtered_data'] = st.session_state['filtered_data'].loc[(st.session_state['filtered_data']['StopSequence'] >= start_stop_sequence) & (
        st.session_state['filtered_data']['StopSequence'] <= end_stop_sequence)]


with st.expander("See data"):
    st.write(st.session_state['filtered_data'])

filter_data_records = st.session_state['filtered_data'].sort_values(
    by="StopSequence").to_dict(orient="records")


def get_delay(arrival_delay, departure_delay):
    if np.isnan(arrival_delay):
        return [int(round(departure_delay / 60, 0)), int(departure_delay)]

    return [int(round(arrival_delay / 60, 0)), int(arrival_delay)]


if len(filter_data_records) > 0 and st.session_state['active_route']:
    COLS_PER_ROW = 4
    rows = [st.columns(COLS_PER_ROW)
            for i in range(math.ceil(len(filter_data_records) / COLS_PER_ROW))]

    for row_nr, row in enumerate(rows):
        for col_nr, col in enumerate(row):
            element_idx = row_nr*COLS_PER_ROW + col_nr

            if element_idx > len(filter_data_records) - 1:
                continue

            record = filter_data_records[element_idx]

            delay_min, delay_sec = get_delay(
                record['ArrivalDelay'], record['DepartureDelay'])

            delay_min_before, delay_sec_before = get_delay(filter_data_records[element_idx - 1]['ArrivalDelay'],
                                                           filter_data_records[element_idx - 1]['DepartureDelay'])

            if delay_sec >= 180:
                delay_color = 'red'
                delay_label = 'Delayed'
            elif delay_sec > 30:
                delay_color = 'orange'
                delay_label = 'Technically on time'
            else:
                delay_color = 'green'
                delay_label = 'On time'

            if element_idx == 0:
                with col.container(border=True, height=180):

                    st.badge(delay_label, color=delay_color)
                    st.metric(label=record['StopName'],
                              value=f"{delay_min} min",
                              delta_color='inverse',
                              help=f"{delay_sec} seconds")
            else:
                delta = delay_min - delay_min_before

                if delta == 0:
                    delta_color = 'off'
                else:
                    delta_color = 'inverse'

                with col.container(border=True, height=180):

                    st.badge(delay_label, color=delay_color)
                    st.metric(label=record['StopName'],
                              value=f"{delay_min} min",
                              delta=f"{delta} min",
                              delta_color=delta_color,
                              help=f"{delay_sec} seconds")

    # st.pydeck_chart(load_map())
