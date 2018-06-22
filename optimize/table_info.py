import leancloud
from datetime import datetime

class TableInfoFormLeancloud():
    Tables = leancloud.Object.extend("migrate_tables")
    Columns = leancloud.Object.extend("migrate_columns")
        
    def __init__(self, t, table_info):
        self.name = t.name
        self.mysql_name = table_info["name"]
        self.ignore_columns = table_info["ignore_columns"]
        self.columns = table_info["columns"]

    def gen_leancloud_col_of_table(self,table):
        for col_name, col_info in self.columns:
            column  = self.Columns()
            column.set("mongo_column_name", col_name)
            column.set("name", col_info["name"])
            column.set("type", col_info["type"])
            column.set("default", col_info["default"])
            column.set("nullable", col_info["nullable"])
            column.set("table", table)
            yield column

    def storage_table_info(self):
        table = self.Tables()
        table.set("mongo_table_name", self.name)
        table.set("name", self.mysql_name)
        table.set("ignore_columns", self.ignore_columns)

        try:
            leancloud.Object.save_all(list(self.gen_leancloud_col_of_table(table)))
        except Exception as e:
            print()
            raise e

    @classmethod
    def get_columns_info_of_table(cls, table):
        query = cls.Columns.query
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
    @classmethod
    def get_tables_info(cls):
        query = cls.Tables.query
        tables = query.find()

        return {
            table.get("mongo_table_name"):{
                "name": table.get("name"),
                "ignore_columns": table.get("ignore_columns"),
                "columns": cls.get_columns_info_of_table(table)
            }
            for table in tables
        }
        

class TablesPoll():
    Poll = leancloud.Object.extend("tables_poll_info")
    result_count = 1000

    def __init__(self, table_name):
        self.table_name = table_name
        self.table_poll_info = self.tables_poll_info[table_name]

    def get_query(self):
        now = datetime.now()
        Table = leancloud.Object.extend(self.table_name)
        query1 = Table.query
        query2 = Table.query
        query1.greater_than_or_equal_to('updatedAt',  self.table_poll_info["last_time"])
        query2.less_than('updatedAt', now)
        self.table_poll_info['last_time'] = now
        return leancloud.Query.and_(query1, query2)

    def get_query_result(self, query, skip=0):
        query.skip(self.result_count * skip)
        query.limit(self.result_count)
        result = query.find()
        yield result
        if len(result) == self.result_count:
            self.table_poll_info['time_interval'] /= 2
            skip += 1
            yield from self.get_query_result(query, skip)

        elif skip == 0  and len(result) < self.result_count / 2 : 
            self.table_poll_info['time_interval'] *= 2
        

    @classmethod
    def get_tables_poll_info(cls):
        query = cls.Poll.query
        result = query.find()

        cls.tables_poll_info =  {
            t.get("table_name"): {
                "last_time": t.get("last_time"), 
                "time_interval": t.get("time_interval")
            }
            for t in result
        }

    @classmethod
    def storage_tables_poll_info(cls):
        
        for table_name, poll_info in cls.tables_poll_info.items():
            poll = cls.Poll()
            poll.set("table_name", table_name)
            poll.set("last_time", poll_info["last_time"])
            poll.set("time_interval", poll_info["time_interval"])
            poll.save()
