import sqlite3
from sqlite3 import Error

class Database:
    db_file = False
    conn = False
    case_table = 'covid_cases'
    importer = None

    def __init__(self, db_file):
        self.db_file = db_file

    def connect(self):
        """ create a database connection to a SQLite database """
        conn = None
        err = None
        try:
            conn = sqlite3.connect(self.db_file)
        except Error as e:
            err = e
        finally:
            if conn:
                self.conn = conn
            else:
                self.conn = False
            if err:
                self.close()
                raise Exception(err)
        return self.conn

    def close(self):
        if self.conn:
            self.conn.close()

    def prepare_schema(self):
        self.exec('DROP TABLE IF EXISTS {}'.format(self.case_table))
        self.exec('''CREATE TABLE {}
         (province     CHAR(50),
         country      CHAR(50)                 NOT NULL,
         updated      INTEGER,
         confirmed    INT                      NOT NULL,
         deaths       INT                      NOT NULL,
         recovered    INT                      NOT NULL,
         dltconfirmed INT,
         dltdeaths    INT,
         dltrecovered INT
         );'''.format(self.case_table))


    def prepare(self):
        self.prepare_schema()
        self.create_indices()
    
    def load_data(self, importer):
        data = importer.import_from_csv()
        self.exec('INSERT INTO {} VALUES (?,?,?,?,?,?,0,0,0)'.format(self.case_table), data)
        self.calculate_delta()
    def create_indices(self):
        self.exec('CREATE INDEX idx_country_name ON {}(country)'.format(self.case_table))
        self.exec('CREATE INDEX idx_province_name ON {}(province)'.format(self.case_table))
        self.exec('CREATE INDEX idx_date ON {}(updated)'.format(self.case_table))
    def calculate_delta(self):
        result = self.exec('SELECT DISTINCT country FROM {}'.format(self.case_table))
    def verify_schema(self):
        result = self.exec('SELECT count(*) > 0 FROM sqlite_master where tbl_name = "{}" and type="table"'.format(self.case_table))
        table_found = result.fetchone()
        if table_found[0] == 1:
            return True
        else:
            return False
    def exec(self, query, multiple_rows=None):
        if not self.conn:
            raise Exception('No database connection')
        c = self.conn.cursor()
        if not multiple_rows:
            return c.execute(query)
        else:
            c.executemany(query, multiple_rows)
            self.conn.commit()
            return True
