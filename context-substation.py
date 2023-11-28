import config

from cogniteapi_rts import client_rts
from cognite.client.data_classes.relationships import Relationship

SOURCE_ASSET_ID = 2198629791599118
DESTINATION_ASSET_ID = 3757826019648372

asset = client_rts.assets.retrieve(id=DESTINATION_ASSET_ID)
time_series = client_rts.time_series.list(asset_ids=[SOURCE_ASSET_ID])

for ts in time_series:
    rel = Relationship(external_id=asset.external_id + "_" + ts.external_id)
    rel.data_set_id = config.WIND_FARM_DATA_SET_ID_RST
    rel.source_external_id = asset.external_id
    rel.source_type = "asset"
    rel.target_external_id = ts.external_id
    rel.target_type = "timeSeries"

    print("Create relationship", asset.external_id, ts.external_id)
    client_rts.relationships.create(rel)
