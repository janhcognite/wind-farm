import json
import time
import config

from datetime import datetime
from cogniteapi import client
from cogniteapitsp import client_tsp
from cognite.client.data_classes.sequences import Sequence, SequenceData

# Source data time series contains data for first year of 2018 - 181 days - 260640 minutes
START_DATE_MS = int(datetime.strptime("2018-02-01", '%Y-%m-%d').timestamp()) * 1000
END_DATE_MS = int(datetime.strptime("2018-08-01", '%Y-%m-%d').timestamp()) * 1000

# Fetch data for one week at a time
ONE_WEEK_MS = 86400 * 7 * 1000

COLUMNS_NAMES = [str(i) for i in range(300)]

columns = []

for index in range(0, 300):
    columns.append({"valueType": "DOUBLE", "externalId": str(index)})


def get_empty_dps_dict(start_date_ms):
    start_minute = int((start_date_ms - START_DATE_MS)/60000)

    dps_dict = {}

    for minute in range(0, 10800):
        column_dict = {column_name: config.TS_NO_VALUE for column_name in COLUMNS_NAMES}
        dps_dict[start_minute + minute] = column_dict

    return dps_dict


def download_single_timeseries_data(external_id, start_week):
    print("\nRetrieving time series data for", external_id)

    start_date_ms = START_DATE_MS + ONE_WEEK_MS*start_week

    print(start_date_ms, "Fetching data for", external_id, "week", start_week)
    datapointsArray = client.time_series.data.retrieve_arrays(external_id=external_id, start=start_date_ms, end=start_date_ms + ONE_WEEK_MS - 200)

    dps_dict = get_empty_dps_dict(start_date_ms)

    if len(datapointsArray) > 0:

        max_ts_minute = 0
        for datapoint in datapointsArray:
            ts = datapoint.timestamp - START_DATE_MS
            ts_minute = int(ts / 60000)  # Row

            if ts_minute == 0:
                ts_fifth_of_second = str(int(ts/60000*300))
            else:
                ts_fifth_of_second = str(int(((ts/60000) % ts_minute) * 300))  # Column

            if dps_dict[ts_minute][ts_fifth_of_second] is config.TS_NO_VALUE:
                dps_dict[ts_minute][ts_fifth_of_second] = datapoint.value

            if ts_minute > max_ts_minute:
                max_ts_minute = ts_minute

    print("Max ts", max_ts_minute)

    # Insert datapoints into sequence
    data = []

    for minute in dps_dict.keys():
        column_values = []
        for fifth_of_second in dps_dict[minute].keys():
            column_values.append(dps_dict[minute][fifth_of_second])

        data.append((minute, column_values))

        if minute % 1440 == 0 and minute > 0:
            print("Writing to sequence. Minute", minute)
            client_tsp.sequences.data.insert(external_id=external_id, column_external_ids=COLUMNS_NAMES, rows=data)
            data = []


def find_next_week_to_populate():
    # Load list of source time series from file
    with open("json/wt-time-series.json") as file:
        ts_source_list = list(json.load(file))

    ts_ext_id = ""
    for ts_source in ts_source_list:
        ts_ext_id = ts_source["external_id"]

        index = 0
        start_week = 0

        # Create sequence if it does not already exist
        seq = client_tsp.sequences.retrieve(external_id=ts_ext_id)

        if seq is None:
            print("Creating sequence", ts_ext_id)
            seq = Sequence(external_id=ts_ext_id, name=ts_ext_id, columns=columns)
            client_tsp.sequences.create(seq)
        
        completed = False
        while not completed:
            for index in range(0, 28):
                data: SequenceData = client_tsp.sequences.data.retrieve(external_id=ts_ext_id, start=index*10080, end=index*10080 + 1, column_external_ids=["0"])

                print(index, data.row_numbers)
                if (len(data.row_numbers) == 0):
                    start_week = index
                    completed = True
                    break

        if start_week < 27:
            start_week = index
            break
    
    return ts_ext_id, start_week


def download_one_week_time_series_data():
    ts_ext_id, start_week = find_next_week_to_populate()
    print(ts_ext_id, start_week)

    download_single_timeseries_data(ts_ext_id, start_week)


download_one_week_time_series_data()
# client_tsp.sequences.delete(external_id="V52-WindTurbine.MxC4cr")