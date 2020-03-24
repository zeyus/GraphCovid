import os.path
import sys
from modules.db.db import Database
from modules.importer.importer import Importer

answer_yes = False
script_dir = os.path.dirname(os.path.realpath(__file__))
covid_db = os.path.join(script_dir, 'data', 'covid.db')
data_source_dir = os.path.join(script_dir, 'data', 'COVID-19', 'csse_covid_19_data', 'csse_covid_19_daily_reports')

if __name__ == "__main__":
    db = Database(covid_db)
    if not os.path.isfile(covid_db):
        if not answer_yes:
            create_db = input('Database not found, would you like to create it? (Y/n)')
            if create_db.lower == 'n':
                print('No problems, bye!')
                exit()
        print('Creating sqlite database "{}"...'.format(covid_db))
        db.connect()
        db.prepare()
        importer = Importer(data_source_dir)
        db.load_data(importer)
        importer = None
        print('Database created.')
    else:
        print('Connecting to database "{}"'.format(covid_db))
        db.connect()
        if not db.verify_schema():
            print('Invalid schema, preparing database...')
            importer = Importer(data_source_dir)
            db.load_data(importer)
            importer = None
            print('DB prepared.')
    result = db.exec('select sum(confirmed) from {}'.format(db.case_table))
    confirmed = result.fetchone()
    print('{} confirmed cases'.format(confirmed))
    db.close()

