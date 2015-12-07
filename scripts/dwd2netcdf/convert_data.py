#!/usr/bin/env python2

'''
Description:    Convert DWD zipfile data to netcdf
Author:         Ronald van Haren, NLeSC (r.vanharen@esciencecenter.nl)
Created:        -
Last Modified:  -
License:        Apache 2.0
Notes:          -
'''

import os
import glob
import fnmatch
import zipfile
import csv
import pandas
from numpy import hstack
from numpy import sort
from numpy import zeros
import numpy as np
from datetime import datetime

datadir = 'data'

def get_variables():
  '''
  get variables/directories from ftp listing
  '''
  dirs = os.listdir(datadir)
  return dirs

def get_list_of_stations(dirs):
  '''
  extract station ids from ftp filenames, create unique list for all 
  variables combined
  '''
  ids = []
  for dir0 in dirs:
    # list of files in current directory
    files = glob.glob(os.path.join(datadir, dir0, '*.zip'))
    ids = ids + [ filename[-32:-27] for filename in files if filename[-32:-27]
           not in ids ]
  return ids

def find_station_files(stationid):
  '''
  find all zip files belonging to a given stationid
  '''
  matches = []
  for root, dirnames, filenames in os.walk(datadir):
      for filename in fnmatch.filter(filenames, '*' + '_' + stationid + '_' +
                                     '*.zip'):
          matches.append(os.path.join(root, filename))
  return matches

def load_file(station_zip):
  '''
  load data files inside zip file and return data in a dictionary
  '''
  # load zipfile
  zipf = zipfile.ZipFile(station_zip)
  # list of files in zip
  data_files = [ filename for filename in zipf.namelist() if 'produkt_' 
                in filename ]
  metadata_files = [ filename for filename in zipf.namelist() if
                    'Stationsmetadaten' in filename ]
  for filename in data_files:
    station_dict = read_data(zipf.open(filename))
  for metadata_file in metadata_files:
    meta_dict = pandas.read_csv(zipf.open(metadata_file), engine='c', sep=';',
                                skipinitialspace=True,
                                header=0).to_dict(orient='records')
  return station_dict, meta_dict


#def read_data(filename):
#  csvdict = pandas.read_csv(filename, engine='c', sep=';', dtype='a',
#                            parse_dates=[' MESS_DATUM'],
#                            header=0)[:-1].to_dict(orient='list')
#  # remove some keys from dict
#  csvdict.pop('eor', None)
#  csvdict.pop('STATIONS_ID', None)
#  csvdict.pop(' QUALITAETS_NIVEAU', None)
#  return csvdict

def read_data(filename):
  '''
  Read csv data and return dictionary: index->
  '''
  csvdict = pandas.read_csv(filename, engine='c', sep=';',
                            parse_dates=['MESS_DATUM'], index_col=['MESS_DATUM'],
                            header=0, skipinitialspace=True).to_dict(
                            orient='index')
  return csvdict

def merge_dicts(*dict_args):
    '''
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    '''
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result

def merge(a, b, path=None):
    "merges b into a"
    if path is None: path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass # same leaf value
            else:
                #raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
                pass  # keep original keys if keys are the same
        else:
            a[key] = b[key]
    return a
    
def convert_dict(dict_of_dicts):
  time_axis = sort(dict_of_dicts.keys())
  pressure_reduced = zeros(len(time_axis))
  pressure_station = zeros(len(time_axis))
  rltvh = zeros(len(time_axis))
  rltvh = zeros(len(time_axis))
  winddir = zeros(len(time_axis))
  windspeed = zeros(len(time_axis))
  clouds = zeros(len(time_axis))
  precipitation = zeros(len(time_axis))
  temperature = zeros(len(time_axis))
  
  # remove nans from time_axis
  time_axis = time_axis[~np.isnan(time_axis)]

  for idx, c in enumerate(time_axis):
    try:
      pressure_reduced[idx] = dict_of_dicts[c]['LUFTDRUCK_REDUZIERT']
    except KeyError:
      pressure_reduced[idx] = -999
    try:
      pressure_station[idx] = 100 * dict_of_dicts[c]['LUFTDRUCK_STATIONSHOEHE']
    except KeyError:
      pressure_station[idx] = -999
    try:
      rltvh[idx] = dict_of_dicts[c]['REL_FEUCHTE']
    except KeyError:
      rltvh[idx] = -999
    try:
      winddir[idx] = dict_of_dicts[c]['WINDRICHTUNG']
    except KeyError:
      winddir[idx] = -999
    try:
      windspeed[idx] = dict_of_dicts[c]['WINDGESCHWINDIGKEIT']
    except KeyError:
      windspeed[idx] = -999
    try:
      clouds[idx] = dict_of_dicts[c]['GESAMT_BEDECKUNGSGRAD']
    except KeyError:
      clouds[idx] = -999
    try:
      precipitation[idx] = dict_of_dicts[c]['NIEDERSCHLAGSHOEHE']
    except KeyError:
      precipitation[idx] = -999
    try:
      temperature[idx] = dict_of_dicts[c]['LUFTTEMPERATUR']
    except KeyError:
      temperature[idx] = -999

  d = {}
  d['pressure_reduced'] = pressure_reduced
  d['pressure_station'] = pressure_station
  d['rltvh'] = rltvh
  d['winddir'] = winddir
  d['windspeed'] = windspeed
  d['clouds'] = clouds
  d['precipitation'] = precipitation
  d['temperature'] = temperature
  d['time'] = [datetime.strptime(str(int(item)), ('%Y%m%d%H')) for
               item in time_axis]
  return d  

def write_combined_data_netcdf(data, stationid):
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
  ncfile.description = 'DWD ' + str(stationid)
  ncfile.history = 'Created ' + time.ctime(time.time())
  # create time dimension
  timevar = ncfile.createDimension('time', None)
  # create lon/lat dimensions
  lonvar = ncfile.createDimension('longitude', 1)
  latvar = ncfile.createDimension('latitude', 1)
  elevation_var = ncfile.createDimension('elevation', 1)

  # inititalize time axis
  timeaxis = [int(round(netcdftime.date2num(data['time'][idx], units='minutes since 2010-01-01 00:00:00',
                                 calendar='gregorian'))) for idx in range(0,len(data['time']))]
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
  lonvar.axis = 'X'
  lonvar.standard_name = 'longitude'
  lonvar[:] = data['longitude']
  latvar = ncfile.createVariable('latitude',dtype('float32').char,('latitude',))
  latvar.units = 'degrees_north'
  latvar.axis = 'Y'
  latvar.standard_name = 'latitude'
  latvar[:] = data['latitude']

  # elevation
  elevation_var = ncfile.createVariable('elevation', dtype('float32').char, ('elevation',))
  elevation_var.units = 'meters'
  elevation_var.axis = 'Z'
  elevation_var.standard_name = 'height'
  elevation_var[:] = data['elevation']
  
  # create other variables in netcdf file
  for variable in data.keys():
    if variable not in ['time', 'longitude', 'latitude', 'elevation', None]:
      # add variables in netcdf file
      # convert strings to npnan if array contains numbers
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

def list_of_dict_to_dict_of_lists(tmp) :
   #result = {}
   #for d in l :
   #   for k, v in d.items() :
   #      result[k] = result.get(k,[]) + [v] #inefficient
   #return result
   return {key:[item[key] for item in tmp] for key in tmp[0].keys() }

def dict_of_list_to_list_of_dicts(d) :
   if not d :
      return []
   #reserve as much *distinct* dicts as the longest sequence
   result = [{} for i in range(max (map (len, d.values())))]
   #fill each dict, one key at a time
   for k, seq in d.items() :
      for oneDict, oneValue in zip(result, seq) :
        oneDict[k] = oneValue
   return result

def convert_meta_dict(metadata_dicts):
  # find unique metadata information
  metadata = {v['von_datum']:v for v in metadata_dicts}.values()
  # convert list of dicts to dict of lists
  metadata = list_of_dict_to_dict_of_lists(metadata)
  # convert timme to datetime objects
  for timestr in ['von_datum', 'bis_datum']:
    metadata[timestr] = [datetime.strptime(str(int(item)),('%Y%m%d')) if 
                         ~np.isnan(item)  else datetime.now() for item in
                         metadata[timestr]]
  return metadata

def split_data(results, metadata):
  '''
  split station data based on moving station location in time
  '''
  data = []
  for idd in range(0,len(metadata['von_datum'])):
    tmp = [ { key : results[key][idx] for key in results.keys() }
            for idx, x in enumerate(results["time"]) if
            metadata['von_datum'][idd]<=x<=metadata['bis_datum'][idd]]
    if not tmp:
      continue  # no measurements found for time period
    tmp_out = list_of_dict_to_dict_of_lists(tmp)
    tmp_out['longitude'] = metadata['Geogr.Breite'][idd]
    tmp_out['latitude'] = metadata['Geogr.Laenge'][idd]
    tmp_out['elevation'] = metadata['Stationshoehe'][idd]
    data = hstack((data,tmp_out))
  return data

def main()
  dirs = get_variables()
  ids = get_list_of_stations(dirs)
  for st in range(0,len(ids)):
    station_files = find_station_files(ids[st])
    station_dicts = []
    metadata_dicts = []
    for sfile in station_files:
      # load data in list of dicts
      sdict, mdict = load_file(sfile)
      station_dicts = hstack((station_dicts, sdict))
      metadata_dicts = hstack((metadata_dicts, mdict))
    # merge station data dicts
    results = reduce(merge, station_dicts)
    # generate output dictionary
    results = convert_dict(results)
    # convert metadata_dicts
    metadata = convert_meta_dict(metadata_dicts)
    # split station data based on station location movements as specified
    # in the metadata
    r2 = split_data(results, metadata)
    for idx in range(0,len(r2)):
      if idx > 0:
        write_combined_data_netcdf(r2[idx], ids[st] + '_' + str(idx+1))
      else:
        write_combined_data_netcdf(r2[idx], ids[st])


if __name__=="__main__":
  main()


