import leancloud
from  datetime import datetime


class TablesPoll():
    Poll = leancloud.Object.extend("tables_poll_info")
    result_count = 1000
    tables_poll_info = None
    max_time_iterval = 8

    def __init__(self, table_name, last_time, time_interval):
        self.table_name = table_name
        self.last_time = last_time 
        self.time_interval = time_interval

    def get_query(self):
        now = datetime.now()
        Table = leancloud.Object.extend(self.table_name)
        query1 = Table.query
        query2 = Table.query
        query1.greater_than_or_equal_to("updatedAt", self.last_time)
        query2.less_than("updatedAt", now)
        self.last_time = now
        return leancloud.Query.and_(query1, query2)

    def get_query_result(self, query, skip=0):
        query.skip(self.result_count * skip)
        query.limit(self.result_count)
        result = query.find()
        yield result

        if len(result) == self.result_count:
            self.time_interval /= 2
            skip += 1
            yield from self.get_query_result(query, skip)

        elif skip == 0 and len(result) < self.result_count / 2 and self.time_interval * 2 <= self.max_time_iterval:
            self.time_interval *= 2

    def sync_success(self):
        self.tables_poll_info[self.table_name]["last_time"] = self.last_time
        self.tables_poll_info[self.table_name]["time_interval"] = self.time_interval

    @classmethod
    def get_table_poll(cls, table_name):
        last_time,time_interval = cls.tables_poll_info[table_name].values()
        return cls(table_name, last_time, time_interval)

    @classmethod
    def set_max_time_iterval(cls, max_time):
        cls.max_time_iterval = max_time

    @classmethod
    def get_tables_poll_info(cls):
        query = cls.Poll.query
        result = query.find()

        cls.tables_poll_info = {
            t.get("table_name"): {
                "last_time": t.get("last_time"), "time_interval": t.get("time_interval")
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
