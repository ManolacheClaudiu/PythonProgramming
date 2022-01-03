# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import mysql.connector
from mysql.connector import Error
import os
import json
import pandas as pd


def create_connection(host_name, user_name, user_password, database_name):
    connect = None
    try:
        connect = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=database_name
        )
        print("Successfully connected to the MySql Database!")
    except Error as err:
        print(f"Error: '{err}'")

    return connect


def create_database(connection_name, query):
    cursor = connection_name.cursor()
    try:
        cursor.execute(query)
        print("Successfully created database!")
    except Error as err:
        print(f"Error: '{err}'")


def execute_query(connection_name, query):
    cursor = connection_name.cursor()
    try:
        cursor.execute(query)
        connection_name.commit()
        print("Successfully executed query!")
    except Error as err:
        print(f"Error: '{err}'")


def insert_into_table(table, value1, value2, value3, value4):
    sql = "INSERT INTO " + table + " VALUES (" + str(value1) + ", '" + value2 + "'," + value3 + ",'" + value4 + "' )"
    execute_query(connection, sql)


def insert_into_table2(table, value1, value2, value3, value4, value5, value6, value7):
    sql = "INSERT INTO " + table + " VALUES (" + str(value1) + "," + str(value2) + ",'" + value3 + "'," + str(
        value4) + ",'" + value5 + \
          "' ," + str(value6) + ", '" + str(value7) + "' )"
    execute_query(connection, sql)


if __name__ == '__main__':
    connection = create_connection("localhost", "root", "12345678", "ExpenseAlert")
    # drop_database_if_exists = "DROP DATABASE IF EXISTS ExpenseAlert;"
    # # execute_query(connection,drop_database_if_exists)
    create_database_query = "CREATE DATABASE ExpenseAlert "
    create_database(connection, create_database_query)

    create_limits_table = """
       CREATE TABLE limits(
    limit_id INT PRIMARY KEY,
    limit_name VARCHAR(20),
    amount INT NOT NULL,
    category VARCHAR(20)
    );"""
    create_invoice_table = """
    CREATE TABLE invoice(
    invoice_id INT PRIMARY KEY,
    price INT NOT NULL,
    category VARCHAR(40),
    number INT NOT NULL,
    month VARCHAR(15),
    year INT,
    day VARCHAR(20)
    );
    """

    drop_table_limits = "DROP TABLE IF EXISTS limits"
    drop_table_invoice = "DROP TABLE IF EXISTS invoice"

    execute_query(connection, drop_table_limits)
    execute_query(connection, drop_table_invoice)
    execute_query(connection, create_invoice_table)
    execute_query(connection, create_limits_table)

    json_data_limits = pd.DataFrame(columns=["name", "category", "total"])

    file = open("limits.json", "r")
    data = json.load(file)
    administration = data['costs'][0]['administration']
    investment = data['costs'][0]['investment']
    otherExpenses = data['costs'][0]['other expenses']
    print('Administration:')
    index = 0
    category = "administration"
    for i in administration[0]:
        amount = str(administration[0][i])
        insert_into_table("limits", index, i, amount, category)
        index = index + 1
    print()

    print('Investment:')
    category = "investment"
    for i in investment[0]:
        amount = str(investment[0][i])
        insert_into_table("limits", index, i, amount, category)
        index = index + 1
    print()

    print('Other Expenses:', otherExpenses)
    amount = str(otherExpenses)
    category = "others"
    insert_into_table("limits", index, "Other expenses", amount, category)

    print()

    path_to_json = 'invoices'
    json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]
    json_data = pd.DataFrame(columns=["price", "category", "number", "month", "year", "day"])
    for index, js in enumerate(json_files):
        with open(os.path.join(path_to_json, js)) as json_file:
            json_text = json.load(json_file)

            price = json_text['price']
            category = json_text['category']
            number = json_text['number']
            month = json_text['month']
            year = json_text['year']
            day = json_text['day']
            insert_into_table2("invoice", index, price, category, number, month, year, day)
            # here I push a list of data into a pandas DataFrame at row given by 'index'
            json_data.loc[index] = [price, category, number, month, year, day]

    print(json_data)
