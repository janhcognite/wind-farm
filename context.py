import config

from cogniteapi_rts import client_rts
from cognite.client.data_classes.three_d import ThreeDModel, ThreeDModelRevision, ThreeDNode, ThreeDAssetMapping


def get_asset_id(turbine_name, component_name):
    turbine_no = 1 + int(turbine_name.split("_")[3])*3 + int(turbine_name.split("_")[4])
    asset_ext_id = "wt" + str(turbine_no).zfill(2) + "_" + component_name
    asset = client_rts.assets.retrieve(external_id=asset_ext_id)

    return asset


def map_node(turbine_name, nodes, node_name, depth):
    node = [node for node in nodes if node.depth == depth and node.name == node_name][0]
    asset = get_asset_id(turbine_name=turbine_name, component_name=node_name)

    print("Mapping", asset.external_id, "with id", asset.id, "to node", node.id)

    asset_mapping = ThreeDAssetMapping(node_id=node.id, asset_id=asset.id)
    client_rts.three_d.asset_mappings.create(model_id=config.MODEL_3D_ID_RTS, revision_id=config.REVISION_3D_ID_RTS, asset_mapping=asset_mapping)


def contextualize_nodes():
    turbine_nodes = client_rts.three_d.revisions.list_nodes(model_id=config.MODEL_3D_ID_RTS, revision_id=config.REVISION_3D_ID_RTS, depth=1, limit=-1)
    turbine_nodes = [node for node in turbine_nodes if node.name.startswith("[REN-2023]")]

    for turbine_node in turbine_nodes:
        print("\n" + turbine_node.name)

        nodes = client_rts.three_d.revisions.list_nodes(model_id=config.MODEL_3D_ID_RTS, revision_id=config.REVISION_3D_ID_RTS, limit=-1, node_id=turbine_node.id)
        map_node(turbine_node.name, nodes, "tower_top", 5)
        map_node(turbine_node.name, nodes, "nacelle_1", 4)


contextualize_nodes()
