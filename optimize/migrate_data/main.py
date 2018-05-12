import sys
sys.path.append('../')

from models.mongodb import MongoTable
from migrate_data import migrate_data
import yaml
import os


class Args():

    def __init__(self):
        self.dir = "/home/pybeef/workspace/data"

        self.config = "/home/pybeef/workspace/leancloud-backup/gen_schema_config.yml"
        self.table = "_User"
        self.data_dir = "/home/pybeef/workspace/dump_data/"
        self.error_dir = "/home/pybeef/err_table/"
        self.test = True


args = Args()


def main():
    with open(args.config) as f:
        config = yaml.load(f.read())
    ignore_tables = config.get("ignore_tables", [])
#    ignore_tables.extend(["GoldLog", "Message", "Coupon", "Order","_User"])
    if args.table is None:
        total = 0
        for t in MongoTable.iter_tables(args.dir):
            if t.name in ignore_tables:
                continue
            try:
                total += 1
                print(total)
                migrate_data(args,t, config)
            except FileNotFoundError:
                print("Don't have {0} data file".format(t.name))

    elif args.table in ignore_tables:
        print("%s is an ignored table" % args.table)
    else:
        p = os.path.join(args.dir, "%s_all.json" % args.table)
        t = MongoTable.get_table(p)
        print(t.name)
        migrate_data(args, t, config)

def get_tables_info():
    
if __name__ == "__main__":
    main()
