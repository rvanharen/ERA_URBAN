#!/usr/bin/env python2

'''
Description:    Download KNMI zipped ascii data and create a csv file with
                station information.
Author:         Ronald van Haren, NLeSC (r.vanharen@esciencecenter.nl)
Created:        -
Last Modified:  -
License:        Apache 2.0
Notes:          -
'''

from lxml.html import parse
import csv
import urllib2
from lxml import html
import numbers
import json
import os
import utils
from numpy import vstack
import argparse


class get_knmi_reference_data:
    '''
    description
    '''
    def __init__(self, opts):
        self.csvfile = opts.csvfile
        self.outputdir = opts.outputdir
        self.keep = opts.keep
        self.check_output_dir()
        self.station = opts.stationid
        self.get_station_ids()
        print (self.stationids)
        self.download_station_data()
        self.get_station_locations()

    def get_station_ids(self):
        '''
        get all stationids from the KNMI website
        '''
        import re
        url = 'http://projects.knmi.nl/klimatologie/metadata/index.html'
        page = parse(url)
        url_metadata = page.xpath(".//table/tr/td/a/@href")
        station_name_id = [c.text for c in page.xpath(".//table/tr/td/a")]
        stationids = [s.split()[0] for s in station_name_id]
        bad_chars = '(){}<>'
        self.rgx = re.compile('[%s]' % bad_chars)
        station_names = [re.sub(self.rgx, '', " ".join(s.split()[1:])) for s in station_name_id]
        if len(self.station)==0:
            self.stationids = [stationids[idx] + ' - ' + station_names[idx] for
                              idx in range(0,len(stationids))]
        else:
          idx = stationids.index(self.station)
          self.stationids = [stationids[idx] + ' - ' + station_names[idx]]
        

    def download_station_data(self):
        ''''
        download zip files containing csv station data
        (complete time series for all KNMI stations)
        '''
        import re
        url = 'http://www.knmi.nl/nederland-nu/klimatologie/uurgegevens'
        page = parse(url)
        # find location of stations on web page
        num_stations = len(page.xpath("/html/body/main/div[2]/div"))
        station_elements = [page.xpath("/html/body/main/div[2]/div["+str(idx)+"]/div/div/div[2]/table/thead/tr/th") for idx in range(0,num_stations)]
        station_names = [re.sub(self.rgx, '', x[0].text) if len(x)>0 else 'ndf'
                         for x in station_elements]
        for stationid in self.stationids:
            div_id = str(station_names.index(stationid))
            relpaths = page.xpath("/html/body/main/div[2]/div["+div_id+"]/div/div/div[2]/table/tbody/tr/td/a/@href")
            for path in relpaths:
                try:
                    fullpath = "http:" + path
                    request = urllib2.urlopen(fullpath)
                    filename = os.path.basename(path)
                    outputfile = os.path.join(self.outputdir, filename)
                    if self.keep:
                        if os.path.exists(outputfile):
                            # check if filesize is not null
                            if os.path.getsize(outputfile) > 0:
                                # file exists and is not null, continue next iteration
                                continue
                            else:
                                # file exists but is null, so remove and redownload
                                os.remove(outputfile)
                    elif os.path.exists(outputfile):
                        os.remove(outputfile)
                    #save
                    output = open(outputfile, "w")
                    output.write(request.read())
                    output.close()
                except urllib2.HTTPError:
                      print "Error downloading file " + fullpath


    def get_station_locations(self):
        '''
        write station name, id and location to csv file
        '''
        # get station names for stationids
        url = 'http://projects.knmi.nl/klimatologie/metadata/index.html'
        page = parse(url)
        url_metadata = page.xpath(".//table/tr/td/a/@href")
        station_name_id = [c.text for c in page.xpath(".//table/tr/td/a")]
        station_id = [s.split()[0] for s in station_name_id]
        station_names = [" ".join(s.split()[1:]) for s in station_name_id]
        for idx, stationid in enumerate(station_id):
            station_url = os.path.join(os.path.split(url)[0],
                                       url_metadata[idx])
            page = parse(station_url)
            rows = [c.text for c in page.xpath(".//table/tr/td")]
            idx_position = rows.index('Positie:') + 1
            idx_startdate = rows.index('Startdatum:') + 1
            lat, lon = rows[idx_position].encode('UTF-8').replace(
                '\xc2\xb0','').replace(' N.B. ', ',').replace(
                    'O.L.','').strip().split(',')
            lat,lon = self.latlon_conversion(lat,lon)
            idx_elevation = rows.index('Terreinhoogte:') + 1
            elevation = float(rows[idx_elevation].encode('UTF-8').split(' ')[0].replace(',','.'))

            try:
                dataout = vstack((dataout,
                                 [station_id[idx], station_names[idx],
                                  lat, lon, elevation, station_url]))
            except NameError:
                dataout = [station_id[idx], station_names[idx],
                           lat, lon, elevation, station_url]
        header = ['station_id', 'station_name','latitude', 'longitude', 'elevation', 'url']
        dataout = vstack((header, dataout))
        # write to csv file
        utils.write_csvfile(self.csvfile, dataout)

    def latlon_conversion(self, lat, lon):
        '''
        conversion of GPS position to lat/lon decimals
            example string for lat and lon input: "52 11'"
        '''
        # latitude conversion
        latd = lat.replace("'","").split()
        lat = float(latd[0]) + float(latd[1])/60
        # longitude conversion
        lond = lon.replace("'","").split()
        lon = float(lond[0]) + float(lond[1])/60
        return lat,lon

    def check_output_dir(self):
        '''
        check if outputdir exists and create if not
        '''
        if not os.path.exists(self.outputdir):
            os.makedirs(self.outputdir)

if __name__ == "__main__":
    # define argument menu
    description = 'Get data KNMI reference stations'
    parser = argparse.ArgumentParser(description=description)
    # fill argument groups
    parser.add_argument('-o', '--outputdir', help='Data output directory',
                        default=os.path.join(os.getcwd(),'KNMI'),
                        required=False)
    parser.add_argument('-s', '--stationid', help='Station id',
                        default='', required=False, action='store')
    parser.add_argument('-c', '--csvfile', help='CSV data file',
                        required=True, action='store')
    parser.add_argument('-k', '--keep', help='Keep downloaded files',
                        required=False, action='store_true')
    parser.add_argument('-l', '--log', help='Log level',
                        choices=utils.LOG_LEVELS_LIST,
                        default=utils.DEFAULT_LOG_LEVEL)
    # extract user entered arguments
    opts = parser.parse_args()
    # define logger
    logname = os.path.basename(__file__) + '.log'
    logger = utils.start_logging(filename=logname, level=opts.log)
    # process data
    get_knmi_reference_data(opts)
