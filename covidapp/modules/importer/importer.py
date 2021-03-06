import glob
import os
import csv
import dateutil.parser  
import dateutil.tz
from datetime import datetime, time

class Importer:
    data_dir = None
    default_date = None
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.default_date = datetime.combine(datetime.now(), time(0, tzinfo=dateutil.tz.gettz("UTC")))
    def import_from_csv(self):
        files = glob.glob('{}/*.csv'.format(self.data_dir))
        data = self.consolidate_data(files)
        files = None
        return data
    def consolidate_data(self, files):
        data = []
        for csv_file in files:
            header_skipped = False
            col_count = None
            file_name = os.path.basename(csv_file)
            file_date = file_name[6:10] + '-' + file_name[0:2] + '-' + file_name[3:5]
            with open(csv_file, 'r') as f:
                covid_data_reader = csv.reader(f)
                for row in covid_data_reader:
                    if not header_skipped:
                        col_count = len(row)
                        header_skipped = True
                        continue
                    # whhyyyyyyyy different formats
                    if col_count == 6 or col_count == 8:
                        data.append((row[0].strip(), row[1].strip(), self.transform_date(row[2]), self.transform_number(row[3]), self.transform_number(row[4]), self.transform_number(row[5]), file_date))
                    elif col_count == 12:
                        data.append((row[2].strip(), row[3].strip(), self.transform_date(row[4]), self.transform_number(row[7]), self.transform_number(row[8]), self.transform_number(row[9]), file_date))
        return data
    def transform_date(self, date, timestamp=True):
        date_obj = dateutil.parser.parse(date, default=self.default_date, dayfirst=False)
        if timestamp:
            return date_obj.timestamp()
        else:
            return date_obj.strftime('%Y-%m-%d')
    def transform_number(self, number):
        if number == '':
            number = 0
        return number