

class data(object):

    data_dir = ""
    def __init__(file_name,db,database):
        self.file_name = file_name
        self.table_name = translate_table_name(file_name)
        self.col_type = get_columns_type(db,database,self.table_name)
        self.col_info = get_columns_info(db,database,self.table_name)
        self.path_name = os.path.join(data_dir,file_name+'.json')
     
    def gen_test_columns(self):
        with open(self.path_name) as f:
            row = yield from ijson.items(f,"results.item")
            row = translate_columns_name(file_name, row)
            diff_columns = handle_columns(row,col_info, col_type,sql_cols)
            
            yield diff_columns if diff_columns else row
        
    def gen_row(self,rows):
        row = yield from gen_row
        if isinstance(row, tuple):
            pass
        diff_type = handle_data(row,sql_cols)
        yield diff_type if diff_type else row

import asyncio
def migrate_data():

    rows = data(file_name, db, database)
    rows_gen = rows.gen_row()

    
    sql = "INSERT INTO {0}({1}) VALUES {2};".format(
                table_name, ','.join(row.keys()), tuple(row.values())
            )
    
    loop = asyncio.get_event_loop()
    loop.run_until_compile(loop.run_in_executor(None, func, db, sql))
    
    