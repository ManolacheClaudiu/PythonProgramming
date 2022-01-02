# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import mysql.connector
from mysql.connector import Error


def create_connection(host_name, user_name, user_password):
    connect = None
    try:
        connect = mysql.connector.connect(
            host=host_name,
            user=user_name,
            password=user_password
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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    connection = create_connection("localhost", "root", "12345678")
    create_database_query = "CREATE DATABASE ExpenseAlert"
    create_database(connection, create_database_query)
