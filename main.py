# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

import mysql.connector
from mysql.connector import Error
import os, json
import pandas as pd



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

    path_to_json = 'facturi'
    json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]
    jsons_data = pd.DataFrame(columns=["price", "category", "number", "month", "year", "day"])
    for index, js in enumerate(json_files):
        with open(os.path.join(path_to_json, js)) as json_file:
            json_text = json.load(json_file)

            # here you need to know the layout of your json and each json has to have
            # the same structure (obviously not the structure I have here)
            price= json_text['price']
            category = json_text['category']
            number = json_text['number']
            month = json_text['month']
            year = json_text['year']
            day = json_text['day']

            # here I push a list of data into a pandas DataFrame at row given by 'index'
            jsons_data.loc[index] = [price, category, number, month, year, day]

    # now that we have the pertinent json data in our DataFrame let's look at it
    print(jsons_data)

