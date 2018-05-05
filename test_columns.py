import multiprocessing
import ijson
import pymysql
import os.path
import copy
from handle_type import handle_data
from handle_columns import translate_columns_name, handle_columns, _get_columns_default
from conf import (
    translate_table_name,
    get_columns_type,
    get_columns_info,
    dump_data_dir,
    dump_data_file,
    ignore_tables,
)

path_name = ""
table_name = ""
file_name = ""

col_type = {}
col_info = {}
columns_default = {}

user = "root"
password = "root"
host = "localhost"
database = "test"

db = pymysql.connect(user=user, password=password, host=host, database=database)

total = 0


def sql_test(row):
    sql_cols = copy.deepcopy(col_type)
    row = translate_columns_name(file_name, row)
    diff_columns = handle_columns(row, col_info, col_type, sql_cols, columns_default)

    if diff_columns[1]:
        #            print(diff_columns)
        #            print(row)
        return diff_columns

    diff_types = handle_data(row, sql_cols)
    if diff_types:
        return ("diff_types", diff_types)

    sql = "INSERT INTO {0}({1}) VALUES {2};".format(
        table_name, ",".join(row.keys()), tuple(row.values())
    )
    return sql


def test_columns(row):
    sql_cols = copy.deepcopy(col_type)
    row = translate_columns_name(file_name, row)
    diff_columns = handle_columns(row, col_info, col_type, sql_cols, columns_default)

    if diff_columns[1]:
        #        print(diff_columns)
        #        print(row)
        return diff_columns

    return row


def test_pool(rows):
    pool = multiprocessing.Pool()
    async_result = pool.imap(sql_test, rows)
    for row in async_result:
        yield row

    pool.close()


def init(fil_name):
    global total, path_name, table_name, col_type, col_info, file_name, columns_default
    file_name = fil_name
    total = 0

    path_name = os.path.join(dump_data_dir, file_name + ".json")
    table_name = translate_table_name(file_name)
    col_type = get_columns_type(db, database, table_name)
    col_info = get_columns_info(db, database, table_name)
    columns_default = _get_columns_default(file_name)


def diff_of_type(diff_columns, sql_cols):
    return {column: sql_cols[column] for column in diff_columns}


diff_columns_total = 0


diff_columns = set()
diff_types = set()
more_columns = set()


def test():
    global total, diff_columns_total, diff_columns, diff_types, more_columns
    with open(path_name) as f:
        rows = ijson.items(f, "results.item")

        for row in test_pool(rows):
            #            total += 1
            #            print(file_name, ":", total)
            if isinstance(row, tuple):
                if row[0] == "diff_columns":
                    diff_columns_total += 1
                    diff_columns |= row[1]
                elif row[0] == "more_columns":
                    more_columns |= row[1]
                elif row[0] == "diff_types":
                    diff_types |= row[1]
                continue

        #            print(row)
        if not diff_columns and diff_types and more_columns:
            return False

        fi = "*" * 5 + file_name + "*" * 5 + "\n"
        diff_info_fp.write(fi)
        diff_info_fp.write(
            "{0}\tdiff_columns: {1}\n".format(diff_columns_total, diff_columns)
        )
        diff_info_fp.write("more_columns: {0}\n".format(more_columns))
        diff_info_fp.write("diff_types: {0}\n".format(diff_types))
        diff_info_fp.write(str(diff_of_type(diff_columns, col_type)) + "\n\n")
        diff_info_fp.flush()


def test_all_dump_data():
    dir = os.listdir(dump_data_dir)
    bad_file_name = {}
    good_file_name = []
    for file_name in dir:
        file_name = file_name.split(".")[0]
        ignore_tables.extend(
            ["GoldLog", "Message", "Coupon", "Order", "LeanEngineSniper", "_User"]
        )
        # this is good
        ignore_tables.extend(
            [
                "RechargeOrder",
                "Invoice",
                "WithdrawStatus",
                "BusinessHours",
                "MessageType",
                "BeefStatus",
                "LottoLog",
                "OffineControl",
                "MallOrderStatus",
                "WithdrawPyramid",
                "BeefPaymentType",
                "MallBanner",
                "BeefGoldLogType",
                "CashLogType",
                "GoldLogType",
                "GiftLog",
                "Alipay",
                "CreditsType",
                "MarketPrice",
                "CrowdProductUsers",
                "CouponRange",
                "CouponDispatch",
                "IncomeDetail",
                "Discover",
                "CommentBridge",
                "CrowdStatus",
                "Province",
                "OrderStatus",
                "Insurance",
                "MallPaymentType",
                "BankInfo",
                "CommodityType",
                "Retrospect",
                "BusinessInfo",
                "Beef",
                "LjStore",
                "Announcement",
                "City",
                "BeefCardSale",
                "wxAccessToken",
                "Cow",
            ]
        )
        if file_name in ignore_tables:
            continue

        try:
            init(file_name)
            if not test():
                good_file_name.append(file_name)
        except Exception as e:
            #            print('BAD',file_name)
            bad_file_name[file_name] = e
            continue

    diff_info_fp.write("BAD_file_name: \n")
    for i in bad_file_name:
        diff_info_fp.write(str(i) + "\n")
    diff_info_fp.write(str(good_file_name))


def testcc():
    for file_name in ["GoldLog", "Message", "Coupon", "Order", "LeanEngineSniper","_User"]:
        try:
            init(file_name)
            test()
        except:
            s = "Bad: " + file_name + "\ndiff_columns:\n" + diff_columns + "\ndiff_types: \n" + diff_types
            diff_info_fp.write(s)
            diff_info_fp.flush()


if __name__ == "__main__":
    diff_info_fp = open("/home/pybeef/big_table", "a")
    try:
        #        test_all_dump_data()

        #        init('_User')
        #        test()
        testcc()
    finally:

        diff_info_fp.close()
