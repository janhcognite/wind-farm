from cogniteapi_rts import client
from cognite.client.data_classes.raw import DatabaseList, TableList, RowList

DB_NAME = "wt_time_series"


def create_db():
    db_list: DatabaseList = client.raw.databases.list(limit=-1)
    ts_db = next((db for db in db_list if db.name == DB_NAME), None)

    if ts_db is not None:
        print("Deleting database", DB_NAME)
        client.raw.databases.delete(DB_NAME)

    print("Creating database", DB_NAME)
    client.raw.databases.create(DB_NAME)


def create_table(name):
    table_list: TableList = client.raw.tables.list(db_name=DB_NAME, limit=-1)
    ts_table = next((table for table in table_list if table.name == name), None)

    if ts_table is not None:
        print("Deleting table", name)
        client.raw.tables.delete(db_name=DB_NAME, name=name)

    print("Creating table", name)
    client.raw.tables.create(db_name=DB_NAME, name=name)


def insert_row_time_series_data(table_name, row_list: RowList):
    client.raw.rows.insert(db_name=DB_NAME, table_name=table_name, row_list=row_list)
