import sys
import os.path
sys.path.append("../")

from libs.tables_info import  gen_table_info,\
                 storage_table_info,get_tables_config
from models.mongodb import MongoTable
from migrate_data import migrate_data


def main(args):
    config = get_tables_config(args.config, args.migrate_config)
    ignore_tables = config.get("ignore_tables", [])
    if args.total != 1:
        # 由于GoldLog表过大
        # 如果运行多个实例，需要一个实例来单独运行GoldLog表
#        ignore_tables.extend(["GoldLog"])
        pass
    if args.table is None:
        for i, t in enumerate(MongoTable.iter_tables(args.dir)):
            if t.name in ignore_tables or i not in range(args.count, 1000, args.total):
                continue

            try:
                table_info = gen_table_info(t, config)
#                storage_table_info(t, table_info)
                migrate_data(args, t, table_info)

            except Exception as e:
                print("[Error] migrate {0} data fault".format(t.name))
                print(e)
                continue

    elif args.table in ignore_tables:
        print("%s is an ignored table" % args.table)

    else:
        p = os.path.join(args.dir, "%s_all.json" % args.table)
        t = MongoTable.get_table(p)
        print(t.name)
        table_info = gen_table_info(t, config)
#        storage_table_info(t, table_info)
        migrate_data(args, t, table_info)


class Args():

    def __init__(self):
        self.dir = "/home/pybeef/workspace/data"

        self.config = "/home/pybeef/workspace/leancloud-backup/gen_schema_config.yml"
        self.migrate_config = "/home/pybeef/workspace/leancloud-backup/data_migration_config.yml"
        self.table = "GoldLog"
        self.data_dir = "/home/pybeef/workspace/dump_data/"
        self.fault_dir = "/home/pybeef/err_table/"
        self.test = True
        self.total = 2
        self.count = 1


if __name__ == "__main__":

    args = Args()
    main(args)
