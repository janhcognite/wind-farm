from datetime import datetime
from cogniteapi import client
from cognite.client.data_classes.raw import RowList, Row

# Source data time series contains data for first year of 2018
START_DATE_MS = int(datetime.strptime("2018-02-01", '%Y-%m-%d').timestamp()) * 1000
END_DATE_MS = int(datetime.strptime("2018-08-01", '%Y-%m-%d').timestamp()) * 1000

# Fetch data for one week at a time
ONE_WEEK_MS = 86400 * 7 * 1000


def copy_one_timeseries_data(external_id):
    print("\nRetrieving time series data for", external_id)

    start_date_ms = START_DATE_MS
    prev_key = 0
    total_source = 0
    complete = False

    while not complete:
        datapointsArray = client.time_series.data.retrieve_arrays(external_id=external_id, start=start_date_ms, end=start_date_ms + ONE_WEEK_MS)
        total_source += len(datapointsArray)

        if len(datapointsArray) > 0:
            row_list = []

            for datapoint in datapointsArray:
                key = datapoint.timestamp - START_DATE_MS
                if key - prev_key >= 100:
                    row_list.append(Row(key=str(key), columns={"value": datapoint.value, }))
                    prev_key = key

            print("Time", start_date_ms, "Source", len(datapointsArray), "Total Source", total_source, "Key", key, "Row List", len(row_list))

        # Increment start date with one week
        start_date_ms += ONE_WEEK_MS

        # Check if all data have been read
        if start_date_ms > END_DATE_MS:
            complete = True


copy_one_timeseries_data("V52-WindTurbine.MyTT")
