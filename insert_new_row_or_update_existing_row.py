from sqlalchemy import create_engine
import pandas as pd
import psycopg2
from datetime import datetime, date
import pytz


def update_table(table_name, values_dict):

    # Get the current date
    today_date = datetime.now(pytz.timezone('America/New_York')).date()
    print(today_date)

    # Setup connection to database
    connection = psycopg2.connect(user="postgres",
                                  password="YOURPASSWORDGOESHERE",
                                  host="192.168.1.98",
                                  port="5432",
                                  database="yf_penny_list")

    # Create a cursor object to execute queries against the database
    cursor = connection.cursor()

    ###################################################################################################
    # FIRST, CHECK IF ROW EXISTS, IF IT DOES, THEN UPDATE IT WITH LATEST VALUES
    ###################################################################################################

    try:
        # SQL Statement to update a single row
        sql = f'UPDATE {table_name} SET ' \
              'date=%s, ' \
              'symbol=%s, ' \
              'last=%s, ' \
              'change=%s ' \
              'WHERE ' \
              'date=%s '

        # Get next index value if adding new row with index
        # index_value = int((pd.read_sql(f'SELECT MAX(index) FROM {symbol.lower()}_al', con=db)).iloc[-1]['max'])

        # Values for the row to be updated
        val = (today_date,
               values_dict['symbol'],
               values_dict['last'],
               values_dict['change'],
               today_date)

        # Execute the SQL statement
        cursor.execute(sql, val)

        # Commit the changes to the database
        connection.commit()

        # Print the number of rows updated
        count = cursor.rowcount
        print(count, "Record(s) updated successfully ")

        ###################################################################################################
        # SECOND, IF THE ROW DOES NOT EXIST, THEN ADD A NEW ROW TO THE TABLE
        ###################################################################################################

        # SQL Statement to insert a new row into the table
        if count == 0:
            print('No existing record to update, adding new record')

            # SQL Statement to insert a new row into the table
            sql = f"INSERT INTO {table_name} (date, symbol, last, change) VALUES (%s, %s, %s, %s)"

            # Values for the row to be inserted
            val = (today_date,
                   values_dict['symbol'],
                   values_dict['last'],
                   values_dict['change'])

            # Execute the SQL statement
            cursor.execute(sql, val)

            # Commit the changes to the database
            connection.commit()

            # Print the number of rows inserted
            count = cursor.rowcount
            print("- ", count, "Record(s) inserted successfully ")

    except (Exception, psycopg2.Error) as error:
        print("Error in update operation", error)

    finally:
        # Close communication with the database
        if connection:
            cursor.close()
            connection.close()
            print("Connection closed")


update_table('test', {'symbol': 'aa', 'last': '22', 'change': '332'})
