import mysql.connector
import pandas as pd
import os
from sqlalchemy import create_engine

# read all file csv
directory = os.path.join("C:\\Users\\PC\\PycharmProjects\\ImportData\\venv\\data\\")


def rename_file():
    directory = os.path.join("C:\\Users\\PC\\PycharmProjects\\ImportData\\venv\\data\\")
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".csv"):
                file_new = file.replace("-", "_")
                # file_new = file.replace(" ", "")
                os.rename(directory + file, directory + file_new)


def connnect_data():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="thanhdat1905",
        database="fooddata")
    return mydb


# prepare column name
def prepare_column():
    column_type_string = [0, 1, 2, 3]
    count = -1
    data_column = pd.read_csv(directory + 'colum.csv')
    # print(data_column['0'])

    sql_column_string = '(';
    for column in data_column['0']:
        count += 1
        column = column.replace("-", "_")
        if count in column_type_string:
            sql_column_string += column + ' TEXT,'
        else:
            sql_column_string += column + ' INT,'
    sql_column_string = sql_column_string[:-1]
    sql_column_string += ')'
    return sql_column_string


# prepare column name
def prepare_column_name():
    column_array = []
    data_column = pd.read_csv(directory + 'colum.csv')
    # print(data_column['0'])

    for column in data_column['0']:
        column = column.replace("-", "_")
        column_array.append(column)
    return column_array


def create_table():
    connect = connnect_data()
    mycursor = connect.cursor()
    sql_string_create_table = "CREATE TABLE ";
    # directory = os.path.join("C:\\Users\\PC\\PycharmProjects\\ImportData\\venv\\data\\")
    for root, dirs, files in os.walk(directory):
        for file in files:
            file = file.replace("-", "_")
            file = file.replace(" ", "")
            if (file == 'colum.csv'):
                continue
            table_name = file[:-4]
            # sql string create table
            # print(table_name)
            sql_string_create_table += table_name + prepare_column() + ";";
            mycursor.execute(sql_string_create_table)


# def import_data():
def prepare_sql_insert_query(table_name):
    data_column = pd.read_csv(directory + 'colum.csv')
    # print(data_column['0'])
    mySql_insert_query = 'INSERT INTO ' + table_name + '(';
    for column in data_column['0']:
        column = column.replace("-", "_")
        mySql_insert_query += column + ","

    mySql_insert_query = mySql_insert_query[:-1]
    mySql_insert_query += ')' + 'VALUES (';
    for column in data_column['0']:
        mySql_insert_query += '"%s",';
    mySql_insert_query = mySql_insert_query[:-1]
    mySql_insert_query += ')'
    return mySql_insert_query


def import_data():
    connect = connnect_data()
    mycursor = connect.cursor()

    # directory = os.path.join("C:\\Users\\PC\\PycharmProjects\\ImportData\\venv\\data\\")
    for root, dirs, files in os.walk(directory):
        for file in files:
            file = file.replace("-", "_")
            file = file.replace(" ", "")
            if (file == 'colum.csv'):
                continue
            table_name = file[:-4]
            sql_string_insert_data = prepare_sql_insert_query(table_name)


def change_header_csv_file():
    new_column = prepare_column_name()
    for root, dirs, files in os.walk(directory):
        for file in files:
            if (file == 'colum.csv'):
                continue
            data = pd.read_csv(directory + file, index_col = False)
            df = pd.DataFrame(data)
            df = df.drop(df.columns[[0]], axis=1)
            df.to_csv(directory + file, header=new_column)


def read_data_csv():
    connect = connnect_data()
    mycursor = connect.cursor()
    engine = create_engine("mysql+pymysql://" + "root" + ":" + "thanhdat1905" + "@" + "localhost" + "/" + "fooddata")
    for root, dirs, files in os.walk(directory):
        for file in files:
            if (file == 'colum.csv'):
                continue
            table_name = file[:-4]
            data = pd.read_csv(directory + file, index_col=0)
            df = pd.DataFrame(data)
            df = df.reset_index(drop=True)
            df.to_sql(table_name.lower(), engine, if_exists='append', index=False)
        #  for row in data.iterrows():
        #      mycursor.execute(prepare_sql_insert_query(table_name),(row))

# done
# read_data_csv()
# print(prepare_column_name())
# rename_file()
# change_header_csv_file()

# connect = connnect_data()
# mycursor = connect.cursor()
# sql_string_create_table = "CREATE TABLE ";
# # directory = os.path.join("C:\\Users\\PC\\PycharmProjects\\ImportData\\venv\\data\\")
# for root, dirs, files in os.walk(directory):
#     for file in files:
#         data = pd.read_csv(directory + file)
#         df = pd.DataFrame(data)
#         file = file.replace("-", "_")
#         file = file.replace(" ", "")
#         if (file == 'colum.csv'):
#             continue
#         table_name = file[:-4]
#         for row in df.head().itertuples():
#             mySql_insert_query = prepare_sql_insert_query(table_name)
#             mycursor.execute(mySql_insert_query, row)
#             connection.commit()
#             print("Record inserted successfully into Laptop table")
#         break

# print(prepare_sql_insert_query('thanhdat'))


# string_sql = "CREATE TABLE " + "test" + prepare_column() + ";";
#
# print(string_sql)

# def read_data():
#     directory = os.path.join("C:\\Users\\PC\\PycharmProjects\\ImportData\\venv\\data\\")
#     for root, dirs, files in os.walk(directory):
#         for file in files:

# mydb = mysql.connector.connect(
#     host="localhost",
#     user="root",
#     password="thanhdat1905",
#     database="fooddata"
#
# )
#
# mycursor = mydb.cursor()
#
# mycursor.execute("SELECT * FROM my_table")
