import yaml
import leancloud

from models.columns import TableInfo
from models.mongodb import MongoTable

leancloud.init("bqyPmlobzR3zYz8CbqrOkmJz-gzGzoHsz", "eBzmtKXRlH9rGsqEi5rLktRp")
Tables = leancloud.Object.extend("migrate_tables")
Columns = leancloud.Object.extend("migrate_columns")
Poll = leancloud.Object.extend("tables_poll_info")


def get_tables_config(config_file, migrate_config_file):
    with open(config_file) as f:
        config = yaml.load(f.read())
    with open(migrate_config_file) as f:
        migrate_config = yaml.load(f.read())
    TableInfo.merge_config(config,migrate_config)
    return config

def gen_table_info(t, config):
    trans = TableInfo(
        common=config["common"], customize=config.get("customize", {}).get(t.name, {})
    )
    return trans(t)


def storage_table_info(t, table_info):

    def gen_col():
        for col_name, col_info in table_info["columns"]:
            column = Columns()
            column.set("mongo_column_name", col_name)
            column.set("name", col_info["name"])
            column.set("type", col_info["type"])
            column.set("default", col_info["default"])
            column.set("nullable", col_info["nullable"])
            column.set("table", table)
            yield column

    Tables = leancloud.Object.extend("migrate_tables")
    table = Tables()
    table.set("mongo_table_name", t.name)
    table.set("name", t.table_info["name"])
    table.set("ignore_columns", table_info["ignore_columns"])

    Columns = leancloud.Object.extend("migrate_columns")
    try:
        leancloud.Object.save_all(list(gen_col()))
    except Exception as e:
        print("Don't storage table_info : %s"%t.name)
        raise e

def get_tables_info():

    def gen_table_info(table):
        query = Columns.query
        query.equal_to("table", table)
        columns = query.find()
        return {
            column.get("mongo_column_name"): {
                "default": column.get("default"),
                "name": column.get("name"),
                "type": column.get("type"),
                "nullable": column.get("nullable"),
            }
            for column in columns
        }

    query = Tables.query
    tables = query.find()

    tables_info = {}
    for table in tables:
        tables_info[table.get("mongo_table_name")] = {
            "name": table.get("name"),
            "ignore_columns": table.get("ignore_columns"),
            "columns": gen_table_info(table),
        }

    return tables_info


def get_tables_poll_info():
    query = Poll.query
    result = query.find()

    return {
        t["table_name"]: {
            "last_time": t["last_time"], "time_interval": t["time_interval"]
        }
        for t in result
    }


def storage_tables_poll_info(tables_poll_info):
    for table_name, poll_info in tables_poll_info.items():
        poll = Poll()
        poll.set("table_name", table_name)
        poll.set("last_time", poll_info["last_time"])
        poll.set("time_interval", poll_info["time_interval"])
        poll.save()
