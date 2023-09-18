from cogniteapi import client
from cognite.client.data_classes.sequences import Sequence

SEQ_EXTERNAL_ID = "V52-WindTurbine.MyTT"

columns = [{"valueType": "LONG", "externalId": "timestamps", "description": "timestamps", "name": "timestamps"},
           {"valueType": "STRING", "externalId": "values", "description": "values", "name": "values"}]

seq = Sequence(external_id=SEQ_EXTERNAL_ID, name=SEQ_EXTERNAL_ID, columns=columns)

# client.sequences.create(seq)

# seqs = client.sequences.list(name=SEQ_EXTERNAL_ID)

# for seq in seqs:
#    print(seq)
#    client.sequences.delete(id=seq.id)

# data = [(1, ['pi',3.14]), (2, ['e',2.72]) ]
# >>> c.sequences.data.insert(column_external_ids=["col_a","col_b"], rows=data, id=1)

text = ""

for i in range(1, 100):
    text += "xxxxxxxxx_"
    print("Length", len(text))
    data = [(1, [0, text])]
    client.sequences.data.insert(external_id=SEQ_EXTERNAL_ID, column_external_ids=["timestamps", "values"], rows=data)