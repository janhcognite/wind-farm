from datetime import datetime

WT_DATA_SET_ID = 3387046938944729

TS_NO_VALUE = 999999999

DATA_SET_ID = 7914362746692288
TOP_ASSET_EXT_ID = "wind_farm_01"
TURBINE_COUNT = 9

JAN_2023_MS = int(datetime.strptime("2023-01-01", '%Y-%m-%d').timestamp()) * 1000
JAN_2024_MS = int(datetime.strptime("2024-01-01", '%Y-%m-%d').timestamp()) * 1000

TURBINE_WEEK_OFFSET = [0, 20, 16, 25, 5, 11, 2, 18, 7]
