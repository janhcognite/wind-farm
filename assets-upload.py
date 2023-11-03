import json
import config

from cogniteapi_rts import client_rts
from cognite.client.data_classes.assets import Asset


def load_source_assets():
    # Open file with asset data
    with open("json/wt-assets.json") as file:
        assets_JSON = list(json.load(file))

    print("Opened", len(assets_JSON), "assets")

    assets = []

    for asset_JSON in assets_JSON:
        asset = Asset()
        asset.name = asset_JSON["name"]
        if "description" in dict(asset_JSON).keys():
            asset.description = asset_JSON["description"].replace("\ufffd", " ")

        asset.external_id = asset_JSON["external_id"]
        # asset.source = asset_JSON["source"]

        if "parent_external_id" in dict(asset_JSON).keys():
            asset.parent_external_id = asset_JSON["parent_external_id"]

        asset.metadata = asset_JSON["metadata"]

        assets.append(asset)

    return assets


def create_new_multi_turbine_asset_data(source_assets, turbine_count):
    # Create top level asset
    assets = [Asset(external_id=config.TOP_ASSET_EXT_ID, name="Wind Farm 01", data_set_id=config.WIND_FARM_DATA_SET_ID_RST)]

    for count in range(1, turbine_count + 1):
        turbine_no = str(count).zfill(2)
        id_prefix = "wt" + turbine_no + "_"
        name_prefix = "WT" + turbine_no + " "

        # Create new assets from source data for this turbine
        for source_asset in source_assets:
            source_asset: Asset
            new_asset = Asset(external_id=id_prefix + source_asset.external_id)
            new_asset.name = name_prefix + source_asset.name
            new_asset.data_set_id = config.WIND_FARM_DATA_SET_ID_RST

            # Parent external id
            if source_asset.parent_external_id is None:
                new_asset.parent_external_id = "wind_farm_01"
            else:
                new_asset.parent_external_id = id_prefix + source_asset.parent_external_id

            # Description
            if source_asset.description is not None:
                new_asset.description = source_asset.description

            # Metadata
            new_asset.metadata = {"turbine": "wt" + turbine_no}

            assets.append(new_asset)

    # for asset in assets:
    #    print(str(asset))

    return assets


def create_new_assets(new_assets):
    # Delete assets if they exist
    top_asset = client_rts.assets.retrieve(external_id=config.TOP_ASSET_EXT_ID)

    if top_asset is not None:
        print("Deleting existing assets")
        client_rts.assets.delete(external_id=config.TOP_ASSET_EXT_ID, recursive=True)

    # Create new assets
    print("Creating", len(new_assets), "assets")
    client_rts.assets.create_hierarchy(assets=new_assets)


source_assets = load_source_assets()
new_assets = create_new_multi_turbine_asset_data(source_assets=source_assets, turbine_count=config.TURBINE_COUNT)

create_new_assets(new_assets)
