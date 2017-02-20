# -*- coding: utf-8 -*-
"""
===============================
eudat.accounting.iRODScollector
===============================

Command line handling
"""

import json
import argparse
import logging
import logging.handlers
import sys
import subprocess

from ConfigParser import SafeConfigParser

from eudat.accounting.client import __version__, LOG, utils
from eudat.accounting.client.__main__ import Application as ApplicationBase

################################################################################
# Configuration Class #
################################################################################


class Configuration(object):
    """
    Get configuration parameters from configuration file
    """

    def __init__(self, file, logger, fileparser):

        self.file = file
        self.logger = logger
        self.fileparser = fileparser

    def parseConf(self):

        """Parse configuration file"""

        print 'Configuration file: %s \n'%self.file

        self.logfile        =  self.fileparser.get('Logging','log_file')
        self.base_url       =  self.fileparser.get('Report','base_url')
        self.domain         =  self.fileparser.get('Report','domain')
        self.account        =  self.fileparser.get('Report','account')
        self.user           =  self.fileparser.get('Report','user'),
        self.password       =  self.fileparser.get('Report','password')
        self.service_uuid   =  self.fileparser.get('Report','service_uuid')
        self.collections    =  self.fileparser.get('Collections','clist')

        #create a file handler
        handler = logging.handlers.RotatingFileHandler(self.logfile, \
                                                   maxBytes=10000000, \
                                                   backupCount=9)
        handler.setLevel(logging.INFO)

        # create a logging format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s\
                                            - %(message)s',"%Y-%m-%d %H:%M:%S")
        handler.setFormatter(formatter)

        # add the handlers to the logger
        self.logger.addHandler(handler)

################################################################################
# EUDAT accounting Class #
################################################################################


class EUDATAccounting(object):
    """Class implementing the computation of statistics about resource consumption."""

    def __init__( self, conf, logger ):
        """Initialize object with configuration parameters."""

        self.conf = conf
        self.logger = logger

    def _query_iCATDb(self):
        """Query iCATdb for number of stored objects and used space in bytes"""

        collections = self.conf.collections.split()
        total_objects = 0
        total_space   = 0
        print "Collections to be accounted:"
        for collection in collections:
            print collection

            try:
                #query size of the collection in bytes
                out=self._raw_query(collection,"DATA_SIZE","sum")

                #Check that output is correct and starting with DATA_SIZE
                if out[:9]=="DATA_SIZE":
                    out     = int(filter(str.isdigit, out))
                    total_space+=int(out)
                    self.logger.info("Storage space for collection: "+collection+\
                                                                ": "+str(out))
                else:
                    self.logger.info("Wrong output for storage space in collection: "\
                                                                    +collection)

                #query number of objects of the collection
                out=self._raw_query(collection,"DATA_ID","count")

                #Check that output is correct and starting with DATA_ID
                if out[:7]=="DATA_ID":
                    out= int(filter(str.isdigit, out))
                    total_objects+=int(out)
                    self.logger.info("number of objects for collection "\
                                                    +collection+": "+str(out))
                else:
                    self.logger.info("Wrong output for object number in collection: "\
                                                                    +collection)

            except Exception,e:
                sys.stdout.write("Exception %s encountered!" % str(e))
                self.logger.info("Exception %s encountered!" % str(e))
                sys.exit(1)

        used_objects= total_objects
        used_space  = total_space

        return used_objects, used_space

    def _raw_query(self,collection,data_type,db_func):

        #query size of the collection in bytes
        query_size="select "+db_func+"("+data_type+") where COLL_NAME like '"\
                                            +collection+"%' |grep "+data_type
        process = subprocess.Popen(["iquest",query_size], stdout=subprocess.PIPE)
        out,err = process.communicate()
        return out

    def _toAccountingRecord(self, stats):
        """Format an eudat accounting record"""

        return {
            'account': self.conf.account,
            'number': stats[0],
            'value': stats[1],
        }

    def reportStatistics(self, args):
        """ Report statistical data on resource consumption to remote server"""

        data = self._query_iCATDb()

        acctRecords = []
        acctRecords.append(self._toAccountingRecord(data))
        # this is a hack - adding the data to the args
        # so other command line args resp their defaults 
        # are available as well
        args.account = acctRecords[0]['account']
        args.value = acctRecords[0]['value']
        args.number = acctRecords[0]['number']
        #data = json.dumps(acctRecords)
        pretty_data = json.dumps(acctRecords, indent=4)
        self.logger.info('Data: ' + pretty_data)

        credentials = utils.getCredentials(self.conf)
        url = utils.getUrl(self.conf)
        data = utils.getData(args)

        if args.test:
            print "Test: Would send the following data: %s" % data
        else:
            response = utils.call(credentials, url, data)
            self.logger.info('Data sent. Status code: ' + str(response.status_code))
            print "\nData sent. Status code: " + str(response.status_code)



def main(argv=sys.argv):
    """Main function called from console command
    """
    logging.basicConfig(filename='.accounting.log', level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    exit_code = 1
    try:
        app = Application(argv)
        app.run()
        exit_code = 0
    except KeyboardInterrupt:
        exit_code = 0
    except Exception as exc:
        LOG.exception(exc)
    sys.exit(exit_code)


class Application(ApplicationBase):
    """The main Application class of the old iRODScollector
    Signature is the same as for the simple client - except for 
    the required arguments."""

    def run(self):
        LOG.info("iRODScollector called with: " + str(self.args))
        print "iRODScollector called with: %s" % str(self.args)

        fileparser = SafeConfigParser()
        fileparser.read(self.args.configpath)

        logger = logging.getLogger('StorageAccounting')
        logger.setLevel(logging.INFO)

        configuration = Configuration(self.args.configpath, logger, fileparser)
        configuration.parseConf()

        eurep = EUDATAccounting(configuration, logger)
        logger.info("Accounting starting ...")
        eurep.reportStatistics(self.args)
        logger.info("Accounting finished")
