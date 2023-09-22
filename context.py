import config

from cogniteapitsp import client_tsp
from cognite.client.data_classes.three_d import ThreeDModel, ThreeDModelRevision, ThreeDNode, ThreeDAssetMapping


def get_asset_id(turbine_name, component_name):
    turbine_no = 1 + int(turbine_name.split("_")[3])*3 + int(turbine_name.split("_")[4])
    asset_ext_id = "wt" + str(turbine_no).zfill(2) + "_" + component_name
    print(asset_ext_id)
    asset = client_tsp.assets.retrieve(external_id=asset_ext_id)

    return asset


turbine_nodes = client_tsp.three_d.revisions.list_nodes(model_id=config.MODEL_3D_ID, revision_id=config.REVISION_3D_ID, depth=1, limit=-1)
turbine_nodes = [node for node in turbine_nodes if len(node.name) > 0]

for turbine_node in turbine_nodes:
    print(turbine_node.name)

    nodes = client_tsp.three_d.revisions.list_nodes(model_id=config.MODEL_3D_ID, revision_id=config.REVISION_3D_ID, limit=-1, node_id=turbine_node.id)
    node = [node for node in nodes if node.depth == 5 and node.name == "tower_top"][0]
    asset = get_asset_id(turbine_name=turbine_node.name, component_name="tower_top")

    print(node.id, asset.id)

    asset_mapping = ThreeDAssetMapping(node_id=node.id, asset_id=asset.id)
    client_tsp.three_d.asset_mappings.create(model_id=config.MODEL_3D_ID, revision_id=config.REVISION_3D_ID, asset_mapping=asset_mapping)
