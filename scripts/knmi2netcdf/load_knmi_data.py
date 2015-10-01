class load_knmi_data:
    def __init__(self, filename):
        self.filename = filename
        self.load_file()
        self.process_reference_data()
        
    def load_file(self):
        '''
        function description
        '''
        import zipfile
        import csv
        import os
        import StringIO
        from numpy import vstack
        # load the zip file
        zipf = zipfile.ZipFile(self.filename)
        # name of csv name in zip file
        txtname = os.path.splitext(os.path.basename(self.filename))[0] + '.txt'
        # read the data in the txt file
        data = StringIO.StringIO(zipf.read(txtname))
        reader = csv.reader(data)
        # file content is ignored while start_data==False
        start_data = False
        # loop through all rows of the txt file
        for row in reader:
            if not start_data:
                if '# STN' in row:  # look for header definition in csv file
                    header = [item.strip() for item in row]
                    # found the header
                    # set start_data = True ==>> use content from here
                    start_data = True
            else:
                # we found the header definition, data starts from here
                if len(row) > 0:
                    # strip data in row and convert to int
                    # empty fiels are filled with -999
                    try:
                        data_row = [int(item.strip()) if item.strip()
                                    else -999 for item in row if item]
                    except ValueError:
                        import pdb; pdb.set_trace()
                    # create array with output data
                    try:
                        csvdata = vstack((csvdata, data_row))
                    except UnboundLocalError:
                        csvdata = data_row
        # create a dictionary from the header and output data
        self.csvdata = dict(zip(header, csvdata.T))

    def process_reference_data(self):
        '''
        process the reference csv data
        '''
        from datetime import datetime
        from datetime import date
        from datetime import timedelta
        from numpy import zeros
        ## Convert time to datetime.datetime object
        # hours should be 0-23 instead of 1-24
        self.csvdata['HH'] = [item if item!=24 else 0 for item in
                              self.csvdata['HH']]
        # combine date and hour into one string
        dstring = [str(self.csvdata['YYYYMMDD'][idx]) +
                   str(self.csvdata['HH'][idx]).zfill(2) for idx in
                   range(0,len(self.csvdata['YYYYMMDD']))]
        # create datetime object
        self.csvdata['datetime'] = [datetime.strptime(
            str(item), ('%Y%m%d%H')) for item in dstring]
        # Correct conversion of the datestring
        # the date of the night -> HH=24 is HH=0 on the next day!
        self.csvdata['datetime'] = [c+ timedelta(days=1) if c.hour==0 else
                                    c for c in self.csvdata['datetime']]
        # rain is (-1 for <0.05 mm), set to 0
        self.csvdata['RH'] = [0 if item == -1 else
                                  item for item in self.csvdata['RH']]
        # process all variables that need to be divided by 10
        for variable in ['T10', 'T', 'RH', 'FF', 'TD']:
            # T10: temperature at 10 cm height, divide by 10 to convert to degC
            # T: temperature at 1.50 m height, divide by 10 to convert to degC
            # RH: rain
            # FF: wind speed
            self.csvdata[variable] = [round(0.1 * item, 1) if item != -999 else
                                item for item in self.csvdata[variable]]
        # SWD
        self.csvdata['Q'] = [round(float(10000 * item)/3600, 5) if
                             item != -999 else item for item in
                             self.csvdata['Q']]
