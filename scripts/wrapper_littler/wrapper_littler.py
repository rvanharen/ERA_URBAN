#!/usr/bin/env python2

# NCEP reanalysis velden 

'''
 description 

'''

# import main packages
import argparse
from namelist import namelist_get, namelist_set
import os

class wrapper_littler:
  '''
  description class
  '''
  def __init__(self,filelist, obsproc_namelist):
    self.filelist = filelist
    self.read_filelist()  # create list of filenames
    self.namelist_obsproc(obsproc_namelist)  # extract time-window
    for idx, filename in enumerate(self.files):  # loop over all files
      self.process_file(filename,idx)  # process file
    self.combine_output_files()  # combine all LITTLE_R files

  def read_filelist(self):
    '''
    read list of files from file
    discard lines with length 0
    add files to list
    '''
    self.files = [line.strip() for line in open(
      self.filelist, 'r') if len(line.strip())>0]

  def namelist_obsproc(self, obsproc_namelist):
    '''
    extract time window from obsproc namelist
    '''
    self.t_min = namelist_get(obsproc_namelist, 'record2:time_window_min')
    self.t_max = namelist_get(obsproc_namelist, 'record2:time_window_max')

  def process_file(self, filename, idx):
    '''
    description
    '''
    import subprocess
    import sys
    # extract time interval from netcdf file using cdo
    
    # remove out.nc file if it exists
    try:
      os.remove('workdir/out.nc')
    except OSError:
      pass
    # extract time interval from input netcdf file, save as out.nc  
    command = 'cdo seldate,' + self.t_min + ',' + self.t_max + ' ' + filename + ' workdir/out.nc'
    print command
    # execute command, catch exceptions
    try:
      # cdo requires shell=True in subprocess.call
      retcode = subprocess.call(command, shell=True, stdout=open(os.devnull, 'wb'))
    except OSError as e:
      print >>sys.stderr, "Execution failed:", e
    
    # edit namelist
    namelist_set('workdir/wageningen_single.namelist', 'group_name:filename', 'out.nc')
    namelist_set('workdir/wageningen_single.namelist', 'group_name:outfile',
                 'results' + str(idx).zfill(3) +'.txt')
    # convert resulting ncdf file to little_R format
    owd = os.getcwd()
    try:
      os.chdir('workdir')
      retcode = subprocess.call('./convert_littler_single', stdout=open(os.devnull, 'wb'))
    except OSError as e:
      print >>sys.stderr, "Execution failed:", e
    finally:
      os.chdir(owd)

  def combine_output_files(self):
    '''
    concatenate all txt files to a single outputfile
    '''
    import fileinput
    import glob
    
    outfilename = 'output.test'
    #filenames = ['workdir/results.txt', 'workdir/results.txt', 'workdir/results.txt']
    filenames = glob.glob('workdir/results*txt')
    with open(outfilename, 'w') as fout:
      for line in fileinput.input(filenames):
        fout.write(line)

if __name__=="__main__":
  wrapper_littler('filelist', '/data/github/WRFDA/var/obsproc/namelist.obsproc')
