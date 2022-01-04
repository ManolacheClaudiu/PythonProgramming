# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import mysql.connector
from mysql.connector import Error
import os
import json
import pandas as pd
import time


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


def select_query(connection_name, query):
    cursor = connection_name.cursor()
    try:
        cursor.execute(query)
        select = cursor.fetchall()
        return select
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


def get_sum(category_name, connection_name):
    sql_sum = "select sum(price) from invoice where category = '" + category_name + "'"
    select_sum = select_query(connection_name, sql_sum)
    return select_sum[0][0]


def get_limit(category_name, connection_name):
    sql_limit = "select amount from limits where category = '" + category_name + "'"
    select_limit = select_query(connection_name, sql_limit)
    return select_limit[0][0]


def import_limits():
    # json_data_limits = pd.DataFrame(columns=["name", "category", "total"])

    file = open("limits.json", "r")
    data = json.load(file)
    administration = data['costs'][0]['administration']
    investment = data['costs'][0]['investment']
    other_expenses = data['costs'][0]['other expenses']
    # print('Administration:')
    index = 0
    category = "administration"
    for i in administration[0]:
        amount = str(administration[0][i])
        insert_into_table("limits", index, i, amount, category)
        index = index + 1

    # print('Investment:')
    category = "investment"
    for i in investment[0]:
        amount = str(investment[0][i])
        insert_into_table("limits", index, i, amount, category)
        index = index + 1

    # print('Other Expenses:', other_expenses)
    amount = str(other_expenses)
    category = "others"
    insert_into_table("limits", index, "Other expenses", amount, category)


def import_invoice(list_of_invoice_name, number_id):
    path_to_json = 'invoices'
    list_of_invoice = []
    json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]
    json_data = pd.DataFrame(columns=["price", "category", "number", "month", "year", "day"])
    # print(json_files)
    index = number_id
    for js in json_files:
        with open(os.path.join(path_to_json, js)) as json_file:
            # print("js:", js)
            if js not in list_of_invoice_name:
                list_of_invoice.append(js)
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
                index = index + 1
    return index, list_of_invoice
    # print(json_data)


def verify_spent_money_amount():
    category = "administration"
    administration_spent = get_sum(category, connection)
    administration_limit = get_limit(category, connection)
    if administration_spent:
        if administration_spent > administration_limit:
            print("YOU SPENT MOORE THAN YOU ESTABLISHED ON  " + category)
    print("On category " + category + " you have the limit " + str(administration_limit) + " and you spent " + str(
        administration_spent))
    category = "investment"
    investment_spent = get_sum(category, connection)
    investment_limit = get_limit(category, connection)
    print("On category " + category + " you have the limit " + str(investment_limit) + " and you spent " + str(
        investment_spent))
    if investment_spent:
        if investment_spent > investment_limit:
            print("YOU SPENT MOORE THAN YOU ESTABLISHED ON " + category)
    category = "others"
    others_spent = get_sum(category, connection)
    others_limit = get_limit(category, connection)
    print(
        "On category " + category + " you have the limit " + str(others_limit) + " and you spent " + str(others_spent))
    if others_spent:
        if others_spent > others_limit:
            print("YOU SPENT MOORE THAN YOU ESTABLISHED ON  " + category)
    print()

    # print(category, get_sum(category, connection), get_limit(category, connection))
    # category = "investment"
    # print(category, get_sum(category, connection), get_limit(category, connection))
    # category = "others"
    # print(category, get_sum(category, connection), get_limit(category, connection))


def select_category(connection_name):
    sql_cat = "select distinct category from limits"
    select_cat = select_query(connection_name, sql_cat)
    return select_cat


def verify_spent_money_amount2(connection_name):
    category = select_category(connection_name)
    for cat in category:
        category_name = cat[0]
        # print(category_name)
        amount_spent = get_sum(category_name, connection_name)
        amount_limit = get_limit(category_name, connection_name)
        if amount_spent:
            if amount_spent > amount_limit:
                print("YOU SPENT MOORE THAN YOU ESTABLISHED ON  " + category_name)
        print("On category " + category_name + " you have the limit " + str(amount_limit) + " and you spent " + str(
            amount_spent))


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

    import_limits()

    list_of_files_name = []
    id_number = 0
    number_of_invoices = len(list_of_files_name)
    while True:
        id_invoice, file_list = import_invoice(list_of_files_name, id_number)
        id_number = id_invoice
        # print(id_number,number_of_invoices)
        if number_of_invoices < id_number:
            verify_spent_money_amount2(connection)
            list_of_files_name.extend(file_list)
            number_of_invoices = len(list_of_files_name)
            print()
        # print("list_of_files_name", list_of_files_name)
        time.sleep(15)
