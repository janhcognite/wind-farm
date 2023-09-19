from cogniteapi import client

WT_ASSET_ID = "WindTurbine"
WT_DATA_SET_ID = 3387046938944729

assets = client.assets.list(data_set_ids=[3387046938944729], limit=-1)

print(assets)

with open('json/wt-assets.json', 'w') as f:
    f.write(str(assets))