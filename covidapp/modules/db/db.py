import sqlite3
from sqlite3 import Error

class Database:
    db_file = False
    conn = False
    case_table = 'covid_cases'
    country_table = 'cases_by_country'
    importer = None
    row_map = {
        'province':     0,
        'country':      1,
        'updated':      2,
        'confirmed':    3,
        'deaths':       4,
        'recovered':    5,
        'dltconfirmed': 6,
        'dltdeaths':    7,
        'dltrecovered': 8,
        'day':          9
    }

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
        self.exec('DROP TABLE IF EXISTS {}'.format(self.country_table))
        self.exec('''CREATE TABLE {}
         (province     CHAR(50),
         country      CHAR(50)                 NOT NULL,
         updated      INTEGER,
         confirmed    INT                      NOT NULL,
         deaths       INT                      NOT NULL,
         recovered    INT                      NOT NULL,
         dltconfirmed INT,
         dltdeaths    INT,
         dltrecovered INT,
         day          CHAR(10)                 NOT NULL
         );'''.format(self.case_table))


    def prepare(self):
        self.prepare_schema()
        self.create_indices()
    
    def load_data(self, importer):
        data = importer.import_from_csv()
        self.exec('INSERT INTO {} VALUES (?,?,?,?,?,?,0,0,0,?) ON CONFLICT DO NOTHING'.format(self.case_table), data)
        self.calculate_delta()
        self.create_country_table()
    def create_indices(self):
        self.exec('CREATE INDEX idx_country_name ON {}(country)'.format(self.case_table))
        self.exec('CREATE INDEX idx_province_name ON {}(province)'.format(self.case_table))
        self.exec('CREATE INDEX idx_date ON {}(updated)'.format(self.case_table))
        self.exec('CREATE INDEX idx_day ON {}(day)'.format(self.case_table))
        self.exec('CREATE INDEX idx_country_day ON {}(country,day)'.format(self.case_table))
        self.exec('CREATE UNIQUE INDEX idx_dcp ON {}(day,country,province)'.format(self.case_table))
    def calculate_delta(self):
        result = self.exec('SELECT DISTINCT country FROM {}'.format(self.case_table))
        countries = result.fetchall()
        result = None
        for country in countries:
            provinces = self.exec('SELECT DISTINCT province FROM {} WHERE country = ?'.format(self.case_table), country, False)
            for province in provinces:
                prev_confirmed = 0
                prev_deaths = 0
                prev_recovered = 0
                query_parameters = []
                data = self.exec('SELECT * FROM {} WHERE province = ? AND country = ? ORDER BY day ASC'.format(self.case_table), [province[0], country[0]], False)
                for entry in data:
                    parameters = (
                        entry[self.row_map['confirmed']] - prev_confirmed,
                        entry[self.row_map['deaths']] - prev_deaths,
                        entry[self.row_map['recovered']] - prev_recovered,
                        entry[self.row_map['day']],
                        entry[self.row_map['country']],
                        entry[self.row_map['province']]
                        )
                    prev_confirmed = entry[self.row_map['confirmed']]
                    prev_deaths = entry[self.row_map['deaths']]
                    prev_recovered = entry[self.row_map['recovered']]
                    query_parameters.append(parameters)
                self.exec('UPDATE {} SET dltconfirmed = ?, dltdeaths = ?, dltrecovered = ? WHERE day = ? AND country = ? AND province = ?'.format(self.case_table), query_parameters)
            print('Updated country "{}"'.format(country[0], province[0]))
    def verify_schema(self):
        result = self.exec('SELECT count(*) > 0 FROM sqlite_master where tbl_name = ? and type="table"', [self.case_table], False)
        table_found = result.fetchone()
        if table_found[0] == 1:
            return True
        else:
            return False
    def create_country_table(self):
        self.exec('CREATE TABLE {} AS SELECT country, day, SUM(confirmed) AS confirmed, SUM(deaths) as deaths, SUM(recovered) AS recovered, SUM(dltconfirmed) AS dltconfirmed, SUM(dltdeaths) AS dltdeaths, SUM(dltrecovered) AS dltrecovered FROM {} GROUP BY country,day'.format(self.country_table, self.case_table))
        print('Created country aggregate table')
    def exec(self, query, parameters=None, multi_query=True):
        if not self.conn:
            raise Exception('No database connection')
        c = self.conn.cursor()
        if not multi_query or not parameters:
            if parameters:
                return c.execute(query, parameters)
            else:
                return c.execute(query)
        else:
            c.executemany(query, parameters)
            self.conn.commit()
            return True
