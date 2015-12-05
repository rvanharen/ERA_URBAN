#!/usr/bin/env python2

'''
Description:    Download historical hourly weather data from DWD ftp server
Author:         Ronald van Haren, NLeSC (r.vanharen@esciencecenter.nl)
Created:        -
Last Modified:  -
License:        Apache 2.0
Notes:          -
'''

from ftplib import FTP
import os

# global variables
global ftphost
ftphost = 'ftp-cdc.dwd.de'

def ftp_connect(ftphost):
  '''
  connect to ftp host
  '''
  ftp = FTP(ftphost)  # connect to host, default port
  ftp.login()  # user anonymous, passwd anonymous@
  return ftp

def getbinary(ftp, filename, ofile):
  '''
  download binary from ftp
  '''
  outfile = open(ofile, 'w')
  ftp.retrbinary("RETR " + filename, outfile.write)
  outfile.close()
  
def ftp_disconnect(ftp):
  '''
  disconnect ftp connection
  '''
  ftp.quit()

def get_dwd_data(ftp):
  '''
  download DWD data from ftp server
  '''
  ftpdir = '/pub/CDC/observations_germany/climate/hourly/'
  datatype = 'historical'
  # change working directory
  ftp.cwd(ftpdir)
  # create list of subdirectories (variables are sort per subdir)
  variables = []
  ftp.dir('-d','*/',lambda L:variables.append(L.split()[-1]))
  variables.remove('solar/')  # nothing to do for this directory
  for variable in variables:
    if not os.path.exists(variable):
      os.makedirs(os.path.join('data', variable))
    ftp.cwd(os.path.join(ftpdir, variable, 'historical'))
    files = ftp.nlst('*.zip')
    [ getbinary(ftp, filename, os.path.join('data', variable, filename)) for
      filename in files ]


if __name__=="__main__":
  ftp = ftp_connect(ftphost)
  get_dwd_data(ftp)
  ftp_disconnect(ftp)

