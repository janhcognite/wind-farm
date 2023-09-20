import util
import config

from cogniteapitsp import client_tsp

SEQ_EXTERNAL_ID = "V52-WindTurbine.MyTT"

seq = client_tsp.sequences.retrieve(external_id=SEQ_EXTERNAL_ID)

for index in range(1, config.TURBINE_COUNT + 1):
    ts_ext_id = util.get_ext_id(SEQ_EXTERNAL_ID, index)
    print(ts_ext_id)

completed = False

time_ms = config.START_INSERT_DATE_JAN_2023
one_day_minutes = 24*60
one_day_ms = 24*60*60*1000
start = 0
end = 800

datapoints = []

while not completed:
    rows = client_tsp.sequences.data.retrieve(external_id=SEQ_EXTERNAL_ID, start=start, end=-1, limit=1440)
    
    index = 0

    for row_no, values in rows.items():
        print("\nRow #\n", row_no)
        index += 1
        for value in values:
            time_ms += 200
            datapoints.append((time_ms, value))

    
    completed = True
    #print(datapoints)
    #print(len(datapoints))
    #client_tsp.time_series.data.insert(datapoints=datapoints)
