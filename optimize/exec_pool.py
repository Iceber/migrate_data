import multiprocessing

class Exec_pool(object):
    data_dir = ""
    error_dir = ""
    pool = multiprocessing.Pool()

    def __init__(self, table,Test = False):
        self.do = self.get_sql if Test else self.get_columns
        self.columns_info = 
        self.data_fp = open(os.path.join(self.data_dir, name+".json"))
        self.rows =ijson.items(self.data_fp,"results.itme")
        self.err_fp = open(os.path.join(self.error_dir,name+".json"),"w")

    
    def get_loop(self):
        result = pool.imap(self.do,self.rows)
        

    def get_sql(self):
        columns 


    def get_err_fp (self):
        return open(os.path.join(error_dir,name),"a")
    

    @staticmethod
    def get_rows(f):
        return ijson.items(f,"results.item")


    def __del__(self):
        self.data_fp.close()
        self.err_fp.close()
