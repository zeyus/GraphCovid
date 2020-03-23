import os.path
import sqlite3
from sqlite3 import Error
import sys

answer_yes = False
script_dir = os.path.dirname(os.path.realpath(__file__))
covid_db = os.path.join(script_dir, 'data', 'covid.db')

def db_connect(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    if not os.path.isfile(covid_db):
        if not answer_yes:
            create_db = input('Database not found, would you like to create it? (Y/n)')
            if create_db.lower == 'n':
                print('No problems, bye!')
                exit()
        print('Creating sqlite database "{}"...'.format(covid_db))
        db_connect(covid_db)
        print('Done')

