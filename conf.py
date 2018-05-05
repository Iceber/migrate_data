import re
import yaml

yaml_path = "/home/pybeef/workspace/leancloud-backup/gen_schema_config.yml"

dump_schema_dir = "/home/pybeef/workspace/data/"
dump_data_dir = '/home/pybeef/workspace/dump_data/'
dump_data_file = 'Cow'


type_default_value = {
    'int' : 0,
    'float':0,
    'varchar':'',
    'json':[],
}


with open(yaml_path) as f:
    config = yaml.load(f.read())
common = config["common"]
customize = config["customize"]
ignore_tables = config["ignore_tables"]

first_cap_re = re.compile("(.)([A-Z][a-z]+)")
all_cap_re = re.compile("([a-z0-9])([A-Z])")


def _snake_case(name):
    s1 = first_cap_re.sub(r"\1_\2", name)
    return all_cap_re.sub(r"\1_\2", s1).lower()


def translate_table_name(file_name):
    table_name = customize.get(file_name, {}).get(
        "table_name", _snake_case(file_name) + "s"
    )
    return table_name

def _handle_sql_columns(col):
    col.pop('id','')
    return col

def get_columns_type(db, database_name, table_name):
    with db.cursor() as cur:
        cur.execute(
            "select  column_name,column_type from information_schema.columns where table_schema =%s  \
                    and table_name = %s;",
            (database_name, table_name)
        )
        col = cur.fetchall()

    return _handle_sql_columns(dict(col))
    
def get_columns_info(db,database_name, table_name):
    with db.cursor() as cur:
        cur.execute(
            "select  column_name,is_nullable,column_default from information_schema.columns where table_schema =%s  \
                    and table_name = %s;",
            (database_name, table_name)
        )
        col = cur.fetchall()
    return _handle_sql_columns({name:(nullable == 'YES', default) for name, nullable, default in col})
