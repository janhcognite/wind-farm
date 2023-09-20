import config

from cogniteapi import client

assets = client.assets.list(data_set_ids=[config.WT_DATA_SET_ID], limit=-1)

print(assets)

with open('json/wt-assets.json', 'w') as f:
    f.write(str(assets))
