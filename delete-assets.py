from cogniteapi_rts import client_rts

print("Deleting old wind farm")
client_rts.assets.delete(id=8049459555240964, recursive=True)

