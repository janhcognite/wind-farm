import util
import time
import config

from cogniteapi_rts import client_rts


def delete_time_series_data(ts_ext_id):
    print("Deleting data for", ts_ext_id)
    client_rts.time_series.data.delete_range(external_id=ts_ext_id, start=config.JAN_2023_MS, end=config.JAN_2024_MS)


def populate_time_series(ts_ext_id, seq_external_id, start, index):
    completed = False
    time_ms = config.JAN_2023_MS
    current_time_ms = int(time.time() * 1000)

    while not completed:
        datapoints = []

        print(ts_ext_id, "week", start)

        # Retrieve source datapoints from Sequence
        rows = client_rts.sequences.data.retrieve(external_id=seq_external_id, start=start*1440*7, end=-1, limit=1440*7)

        # Create list of datapoints. Ignore "None" values equal to 999 999 999.
        for row_no, values in rows.items():
            for value in values:
                time_ms += 200
                if time_ms < current_time_ms:
                    if value != config.TS_NO_VALUE:
                        if time_ms % 3600000 == 0:
                            datapoints.append((time_ms, value))
                else:
                    completed = True
                    break

        # Insert datapoints for one week
        if len(datapoints) > 0:
            client_rts.time_series.data.insert(external_id=ts_ext_id, datapoints=datapoints)

        # Increment week number, wrap to 0 when more than 26
        start = (start + 1) % 27  


def populate_time_series_variants(seq_external_id):
    for index in range(1, config.TURBINE_COUNT + 1):
        # Create the time series external id for this turbine
        ts_ext_id = util.get_ext_id(seq_external_id, index)

        # Delete existing time series data
        delete_time_series_data(ts_ext_id)

        # Create a random number for which week to start pulling data for this turbine
        # random.seed(hash(ts_ext_id))
        # start_week = random.randint(0, 26)
        start_week = config.TURBINE_WEEK_OFFSET[index - 1]

        # Populate time series data
        populate_time_series(ts_ext_id, seq_external_id, start_week, index)


def create_datapoints():
    for seq_external_id in config.prioritized_ts_list:
        populate_time_series_variants(seq_external_id)


create_datapoints()