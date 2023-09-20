import config

from cogniteapi import client

ts_list = client.time_series.list(data_set_ids=[config.WT_DATA_SET_ID], limit=-1)

for ts in ts_list:
    asset_ext_id = client.assets.retrieve(id=ts.asset_id).external_id
    ts.metadata["asset_external_id"] = asset_ext_id
    ts.data_set_id = None
    ts.created_time = None
    ts.last_updated_time = None
    ts.asset_id = None

print(ts_list)


with open('json/wt-time-series.json', 'w') as f:
    f.write(str(ts_list))
