import math
import pytz
import time
import streamlit as st
from helpers import elastic
from datetime import datetime


def handleRouteChange():
    print("Handling Change")
    st.session_state['departure_stop'] = None
    st.session_state['stop_departure_time'] = None


last_entry_ts = elastic.get_most_recent_bucket()

# Init Session State
if "available_routes" not in st.session_state:
    st.session_state['available_routes'] = elastic.fetch_routes()

if "departure_stop" not in st.session_state:
    st.session_state['departure_stop'] = None

if "stop_departure_time" not in st.session_state:
    st.session_state['stop_departure_time'] = None

if "available_departure_stops" not in st.session_state:
    st.session_state['available_departure_stops'] = None


st.header("bettersbb.ch")
# st.write("Check delays of your train, with an accuracy of 6 seconds and before the train is more late more than 3 minutes.")
st.write(
    f"Last data from ~{int(round((datetime.now(tz=pytz.utc) - datetime.fromisoformat(last_entry_ts)).seconds / 60, 0))} mins ago.")

# Data container
with st.container(border=True, key="search_form", height=400):
    st.selectbox(label="Select Route",
                 key='selected_route',
                 on_change=handleRouteChange,
                 options=st.session_state['available_routes'])

    if st.session_state['selected_route']:
        route_info = elastic.fetch_route_info(
            route_short_names=[st.session_state['selected_route']]
        )

        st.pills(
            label="Direction:",
            key='trip_direction',
            options=route_info['trip_headsigns'],
        )

    if st.session_state['trip_direction']:
        st.session_state['available_departure_stops'] = elastic.get_departure_stops(
            routes=[st.session_state['selected_route']],
            trip_headsign=st.session_state['trip_direction']
        )

    if st.session_state['available_departure_stops']:
        st.pills(
            label="Departure Stop:",
            key="departure_stop",
            options=st.session_state['available_departure_stops']
        )

    if st.session_state['departure_stop']:
        stop_departure_times = elastic.fetch_stop_departure_times(
            route_short_name=st.session_state['selected_route'],
            trip_headsign=st.session_state['trip_direction'],
            stop_name=st.session_state['departure_stop']
        )

        st.pills(
            label="Departure at station:",
            key="stop_departure_time",
            options=stop_departure_times
        )

if st.session_state['selected_route'] and st.session_state['trip_direction'] and st.session_state['departure_stop'] and st.session_state['stop_departure_time']:
    trip_id = elastic.fetch_trip_id(
        route_short_name=st.session_state['selected_route'],
        trip_headsign=st.session_state['trip_direction'],
        stop_name=st.session_state['departure_stop'],
        stop_departure_time=st.session_state['stop_departure_time'],
    )

    if len(trip_id) > 1:
        st.warning("Multipe Routes IDs found, information might be wrong...")

    with st.container(key="content_container"):
        print("Re-rendering Delays")
        filter_data_records = elastic.fetch_route_delay_historic(
            trip_id=trip_id[0]
        )

        if len(filter_data_records) > 0:
            COLS_PER_ROW = 4
            rows = [st.columns(COLS_PER_ROW)
                    for i in range(math.ceil(len(filter_data_records) / COLS_PER_ROW))]

            for row_nr, row in enumerate(rows):
                for col_nr, col in enumerate(row):
                    element_idx = row_nr*COLS_PER_ROW + col_nr

                    if element_idx > len(filter_data_records) - 1:
                        continue

                    record = filter_data_records[element_idx]

                    delay_sec = record['delays'][-1]
                    delay_min = int(round(delay_sec / 60))

                    delay_sec_before = filter_data_records[
                        element_idx - 1
                    ]['delays'][-1]
                    delay_min_before = int(round(delay_sec_before / 60, 0))

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
                        with col.container(border=True, height=250):

                            st.badge(delay_label, color=delay_color)
                            st.metric(label=record['stop.stop_name'],
                                      value=f"{delay_min} min",
                                      delta_color='inverse',
                                      chart_data=record['delays'],
                                      chart_type='area',
                                      help=f"{delay_sec} seconds")

                    else:
                        delta = delay_min - delay_min_before

                        if delta == 0:
                            delta_color = 'off'
                        else:
                            delta_color = 'inverse'

                        with col.container(border=True, height=250):

                            st.badge(delay_label, color=delay_color)
                            st.metric(label=record['stop.stop_name'],
                                      value=f"{delay_min} min",
                                      delta=f"{delta} min",
                                      delta_color=delta_color,
                                      chart_data=record['delays'],
                                      chart_type='line',
                                      help=f"{delay_sec} seconds")
