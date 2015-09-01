#!/usr/bin/env python2

'''
description:  
author:       Ronald van Haren, NLeSC (r.vanharen@esciencecenter.nl)
licence:      Apache 2.0
'''

from numpy import concatenate as npconcatenate
import csv

def read_knmi_data(reference_station):
    '''
    Calculate or load KNMI reference data:
        pickled file exists -> load
        pickled file doesn't exist -> calculate
    '''
    from load_knmi_data import load_knmi_data
    import glob
    from numpy import sort
    from numpy import concatenate
    import collections    
    # generate filename of KNMI station
    filenames = sort(glob.glob('KNMI/uurgeg_' + str(reference_station) + '*.zip' ))
    # load all csv files in list of dictionaries
    dicts = [load_knmi_data(filename).csvdata for filename in filenames]
    # merge all dictionaries in a super dictionary
    knmi_data = collections.defaultdict(list)
    for idx in range(0,len(dicts)):
      try:
        knmi_data = dict((k, npconcatenate((knmi_data.get(k), dicts[idx].get(k)))) for k in set(knmi_data.keys() + dicts[idx].keys()))
      except ValueError:
        # cannot concatenate empty arrays
        knmi_data = dict((k, dicts[idx].get(k)) for k in dicts[idx].keys())
    # return dictionary with all variables/time steps
    return knmi_data

def write_combined_data_netcdf(data, stationid, lon, lat):
  '''
  description
  '''
  from netCDF4 import Dataset as ncdf
  import netcdftime
  from datetime import datetime
  from dateutil import tz
  from numpy import zeros
  from numpy import nan as npnan
  from numpy import dtype
  import time
  ncfile = ncdf('output'+str(stationid)+'.nc', 'w', format='NETCDF4')
  # description of the file
  ncfile.description = 'KNMI ' + str(stationid)
  ncfile.history = 'Created ' + time.ctime(time.time())
  # create time dimension
  timevar = ncfile.createDimension('time', None)
  # create lon/lat dimensions
  lonvar = ncfile.createDimension('longitude', 1)
  latvar = ncfile.createDimension('latitude', 1)
  # inititalize time axis
  timeaxis = [netcdftime.date2num(data['datetime'][idx], units='minutes since 2010-01-01 00:00:00',
                                 calendar='gregorian') for idx in range(0,len(data['datetime']))]

  # netcdf time variable UTC
  timevar = ncfile.createVariable('time', 'i4', ('time',),
                                  zlib=True)
  timevar[:] = timeaxis
  timevar.units = 'minutes since 2010-01-01 00:00:00'
  timevar.calendar = 'gregorian'
  timevar.standard_name = 'time'
  timevar.long_name = 'time in UTC'

  # lon/lat variables
  lonvar = ncfile.createVariable('longitude',dtype('float32').char,('longitude',))
  lonvar.units = 'degrees_east'
  lonvar[:] = lon
  latvar = ncfile.createVariable('latitude',dtype('float32').char,('latitude',))
  latvar.units = 'degrees_north'
  latvar[:] = lat

  # create other variables in netcdf file
  for variable in data.keys():
    if variable not in ['YYYMMDD', 'Time', '<br>', 'datetime', '# STN', None]:
      # add variables in netcdf file
      # convert strings to npnan if array contains numbers
      print variable
      if True in [is_number(c)
        for c in data[variable]]:
          data[variable] = [npnan if isinstance(
            fitem(c), str) else fitem(c) for c in data[
              variable]]
      # check if variable is a string
      if not isinstance(data[variable][1], str):
          # fill variable
          variableName = variable
          values = ncfile.createVariable(
            variableName, type(data[variable][1]),
            ('time',), zlib=True, fill_value=-999)
      else:
        # string variables cannot have fill_value
        values = ncfile.createVariable(
          variable, type(data[variable][1]),
          ('time',), zlib=True)
      try:  # fill variable
        values[:] = data[variable][:]
      except IndexError:
        # for strings the syntax is slightly different
        values = data[variable][:]
        #self.fill_attribute_data()


def fill_attribute_data():
  '''
  Function that fills the attribute data of the netcdf file
  '''
  if variable == 'DD':
    values.units = 'degrees'
    values.standard_name = 'wind direction'
    values.long_name = 'mean wind direction during the 10-minute period preceding the time of observation (990=variable)'
  elif variable == 'TemperatureF':
    values.units = 'F'
    values.standard_name = 'air_temperature'
    values.long_name = 'air temperature'
  else:
    pass


def fitem(item):
    try:
        item = item.strip()
    except AttributeError:
        pass
    try:
        item = float(item)
    except ValueError:
        pass
    return item


def is_number(s):
    '''
    check if the value in the string is a number and return True or False
    '''
    try:
        float(s)
        return True
    except ValueError:
        pass
    return False


def load_csv_data(csvfile):
    '''
    load data csvfile
    '''
    with open(csvfile, 'r') as csvin:
        reader = csv.DictReader(csvin, delimiter=',')
        try:
            csvdata
        except UnboundLocalError:
            reader.next()
            try:
                csvdata = {k.strip(): [fitem(v)] for k, v in
                                reader.next().items()}
            except StopIteration:
                pass
        current_row = 0
        for line in reader:
            current_row += 1
            if current_row == 1:  # header
                # skip the header
                continue
            for k, v in line.items():
                if k is not None:  # skip over empty fields
                    k = k.strip()
                    csvdata[k].append(fitem(v))
    return csvdata


if __name__=="__main__":
  knmi_csv_info = load_csv_data('knmi_reference_data.csv')
  station_ids = [int(x) for x in knmi_csv_info['station_id']]
  
  for station in station_ids:
    lat = knmi_csv_info['latitude'][station_ids.index(station)]
    lon = knmi_csv_info['longitude'][station_ids.index(station)]
    data = read_knmi_data(station)
    write_combined_data_netcdf(data, station, lon, lat)
