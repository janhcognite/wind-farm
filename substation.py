from cogniteapi_rts import client_rts
from cognite.client.data_classes.three_d import ThreeDModelRevisionUpdate

SUBSTATION_MODEL_ID = 6340043742599493
SUBSTATION_REVISION_ID = 8211488583658737

revision = client_rts.three_d.revisions.retrieve(model_id=SUBSTATION_MODEL_ID, id=SUBSTATION_REVISION_ID)

print(revision)

revision_upd = ThreeDModelRevisionUpdate(id=SUBSTATION_REVISION_ID)
revision_upd.scale.set([0.01, 0.01, 0.01])
revision_upd.rotation.set([0, 0, 90])
revision_upd.translation.set([1500, -500, 38])

update = client_rts.three_d.revisions.update(model_id=SUBSTATION_MODEL_ID, item=revision_upd)

print(update)