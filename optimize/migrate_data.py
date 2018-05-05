from mongodb import MongoTable
from columns import Columns_info


def migrate_data(rows):
    with open(args.config) as f:
        config = yaml.load(f.read())
    ignore_tables = config.get("ignore_tables",[])


    if args.table is None:
        for t in MongoTable.iter_tables(args.dir):
            if t.name in ignore_tables:
                continue
            
            columns_info = Columns_info(config["common"],config.get("customize",{}).get(t.name,{}))
            columns_info = columns_info(t)

    elif args.table in ignore_tables:
        print()
    else:
        













def test():
    with open(path_name) as f:
        rows = ijson.items(f, "results.item")

        pool = multiprocessing.Pool()
        rows = pool.imap(,rows)
        for row in rows:
            if isinstance(row,tuple):
                if rows[0] == "diff_columns":
                    diff_columns |= row[1]
                elif rows[0] == "more_columns":
                    more_columns|= rows[1]
                elif row[0] == "diff_types":
                    diff_types |= row[1]
                continue

        insert_data(row)        
