from cogniteapitsp import client_tsp
from cognite.client.data_classes.sequences import Sequence

SEQ_EXTERNAL_ID = "V52-WindTurbine.MyTT"

columns = []

for index in range(0, 300):
    columns.append({"valueType": "DOUBLE", "externalId": str(index)})


seq = Sequence(external_id=SEQ_EXTERNAL_ID, name=SEQ_EXTERNAL_ID, columns=columns)

# client_tsp.sequences.create(seq)

# seqs = client.sequences.list(name=SEQ_EXTERNAL_ID)

# for seq in seqs:
#    print(seq)
#    client.sequences.delete(id=seq.id)

# data = [(1, ['pi',3.14]), (2, ['e',2.72]) ]
# >>> c.sequences.data.insert(column_external_ids=["col_a","col_b"], rows=data, id=1)

text = ""

#seq = client_tsp.sequences.retrieve(external_id=SEQ_EXTERNAL_ID)

rows = client_tsp.sequences.data.retrieve(external_id=SEQ_EXTERNAL_ID, start=240000, end=2500000, column_external_ids=["0"])

print(rows)