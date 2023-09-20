from datetime import datetime
from cogniteapi import client
from cogniteapitsp import client_tsp

# Source data time series contains data for first year of 2018 - 181 days - 260640 minutes
START_DATE_MS = int(datetime.strptime("2018-02-01", '%Y-%m-%d').timestamp()) * 1000
END_DATE_MS = int(datetime.strptime("2018-08-01", '%Y-%m-%d').timestamp()) * 1000

# Fetch data for one week at a time
ONE_WEEK_MS = 86400 * 7 * 1000

COLUMNS_NAMES = [str(i) for i in range(300)]


def get_empty_dps_dict(start_date_ms):
    start_minute = int((start_date_ms - START_DATE_MS)/60000)

    dps_dict = {}

    for minute in range(0, 10800):
        column_dict = {column_name: 999999999 for column_name in COLUMNS_NAMES}
        dps_dict[start_minute + minute] = column_dict

    return dps_dict


def copy_one_timeseries_data(external_id):
    print("\nRetrieving time series data for", external_id)

    start_date_ms = START_DATE_MS
    complete = False

    while not complete:
        print(start_date_ms, "Fetching data")
        datapointsArray = client.time_series.data.retrieve_arrays(external_id=external_id, start=start_date_ms, end=start_date_ms + ONE_WEEK_MS)

        dps_dict = get_empty_dps_dict(start_date_ms)
        print(len(dps_dict.keys()))

        if len(datapointsArray) > 0:
            
            for datapoint in datapointsArray:
                ts = datapoint.timestamp - START_DATE_MS
                ts_minute = int(ts / 60000)  # Row

                # if ts_minute == 1000:
                #    break

                if ts_minute == 0:
                    ts_fifth_of_second = str(int(ts/60000*300))
                else:
                    ts_fifth_of_second = str(int(((ts/60000) % ts_minute) * 300))  # Column

                # if ts_minute not in dps_dict.keys():
                #    dps_dict[ts_minute] = dict.fromkeys(COLUMNS_NAMES)

                if dps_dict[ts_minute][ts_fifth_of_second] is None:
                    dps_dict[ts_minute][ts_fifth_of_second] = datapoint.value

            # print("Time", start_date_ms, "Source", len(datapointsArray), "Total Source", total_source, "Key", key, "Row List", len(row_list))

        # print(json.dumps(dps_dict, indent=4))

        # Insert datapoints into sequence
        data = []

        for minute in dps_dict.keys():
            column_values = []
            for fifth_of_second in dps_dict[minute].keys():
                column_values.append(dps_dict[minute][fifth_of_second])

            # if column_values.count(None) == 299:
            #    column_values = [-999999] * 300
            
            data.append((minute, column_values))

            # print(int((start_date_ms-START_DATE_MS)/60000), "Inserting data into sequence", external_id)
            if minute % 1440 == 0:
                print("Writing to sequence. Minute", minute)
                client_tsp.sequences.data.insert(external_id=external_id, column_external_ids=COLUMNS_NAMES, rows=data,)
                data = []

        # Increment start date with one week
        start_date_ms += ONE_WEEK_MS

        # Check if all data have been read
        if start_date_ms > END_DATE_MS:
            complete = True

# data = [(1, ['pi',3.14]), (2, ['e',2.72]) ]
# >>> c.sequences.data.insert(column_external_ids=["col_a","col_b"], rows=data, id=1)


copy_one_timeseries_data("V52-WindTurbine.MyTT")
