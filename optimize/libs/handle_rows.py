from libs._handle_columns import filter_columns,handle_columns,handle_data


def trans_row(row, columns):
    return {columns[name]["name"]: value for name,value in row }

def handle_row(row,table_info,test = False):
    filter_columns(row, table_info["ignore_columns"])
    fault_columns = handle_columns(row, table_info["columns"])
    if any(fault_columns.values()):
        return (fault_columns,)
    fault_types = handle_data(row, table_info["columns"])
    fault_columns.update(fault_types)
    if test or any(fault_columns.values()):
        return (fault_columns,)
    return row