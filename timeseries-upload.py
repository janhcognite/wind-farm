import json
import config

from cogniteapi_tsp import client_tsp
from cognite.client.data_classes.time_series import TimeSeries


def load_source_time_series():
    # Open file with time series data
    with open("json/wt-time-series.json") as file:
        ts_source_list = list(json.load(file))

    print("Opened ", len(ts_source_list), "time series")

    ts_list = []

    for ts_source in ts_source_list:
        ts = TimeSeries()
        ts.name = ts_source["name"]
        if "description" in dict(ts_source).keys():
            ts.description = ts_source["description"]

        ts.external_id = ts_source["external_id"]

        ts.metadata = ts_source["metadata"]

        ts_list.append(ts)

    return ts_list


def create_new_multi_turbine_time_series_data(ts_source_list, turbine_count):
    assets = client_tsp.assets.list(data_set_ids=[config.DATA_SET_ID], limit=-1)

    ts_list = []

    for count in range(1, turbine_count + 1):
        turbine_no = str(count).zfill(2)
        id_prefix = "WT" + turbine_no
        name_prefix = "WT" + turbine_no + " "

        # Create new time series from source data for this turbine
        for ts_source in ts_source_list:
            ts_source: TimeSeries
            ts_new: TimeSeries = TimeSeries(external_id=id_prefix + ts_source.external_id.replace("-WindTurbine", "").replace("V52", ""))
            ts_new.name = name_prefix + ts_source.name
            ts_new.data_set_id = config.DATA_SET_ID

            # Description
            if ts_source.description is not None:
                ts_new.description = ts_source.description

            # Metadata
            ts_new.metadata = ts_source.metadata
            ts_new.metadata["turbine"] = id_prefix.lower()

            # Link to asset
            external_id = "wt" + turbine_no + "_" + ts_source.metadata["asset_external_id"]
            ts_new.asset_id = next((asset.id for asset in assets if asset.external_id == external_id), None)

            ts_list.append(ts_new)

    return ts_list


def create_new_time_series(ts_list):
    # Delete time_series if they exist
    old_ts_list = client_tsp.time_series.list(data_set_ids=[config.DATA_SET_ID], limit=-1)
    old_ts_ids = [ts.id for ts in old_ts_list]

    if len(old_ts_ids) > 0:
        print("hello")

    # Create new time series
    print("Creating", len(ts_list), "time series")
    client_tsp.time_series.create(ts_list)


ts_source_list = load_source_time_series()
ts_list = create_new_multi_turbine_time_series_data(ts_source_list, config.TURBINE_COUNT)
create_new_time_series(ts_list)
