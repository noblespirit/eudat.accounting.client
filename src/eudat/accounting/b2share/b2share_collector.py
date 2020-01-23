# -*- coding: utf-8 -*-
"""
===============================
eudat.accounting.b2share_collector
===============================
"""

import json
import argparse
import logging
import logging.handlers
import sys
import os

try:
    from ConfigParser import SafeConfigParser
except ImportError:
    # Python 3
    from configparser import SafeConfigParser

from eudat.accounting.client import __version__, LOG, utils
from eudat.accounting.client.__main__ import Application as ApplicationBase

from eudat.accounting.b2share.b2share_accounting import B2SHAREAccounting


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

        print('Configuration file: %s \n' % self.file)

        # Configuration provided with config file
        self.logfile = self.fileparser.get('Logging', 'log_file')
        self.base_url = self.fileparser.get('Report', 'base_url')
        self.domain = self.fileparser.get('Report', 'domain')
        self.account = self.fileparser.get('Report', 'account')
        self.user = self.fileparser.get('Report', 'user')
        self.password = self.fileparser.get('Report', 'password')
        self.service_uuid = self.fileparser.get('Report', 'service_uuid')
        self.page_size = self.fileparser.get('Report', 'page_size', fallback=100)
        self.b2share_community = self.fileparser.get('B2SHARE', 'community')
        self.b2share_url = self.fileparser.get('B2SHARE', 'url')

        # Configuration provided with environment variables
        self.api_token = os.getenv('B2SHARE_SUPERADMIN_API_KEY', None)

        # create a file handler
        handler = logging.handlers.RotatingFileHandler(self.logfile, \
                                                       maxBytes=10000000, \
                                                       backupCount=9)
        handler.setLevel(logging.INFO)

        # create a logging format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s\
                                            - %(message)s', "%Y-%m-%d %H:%M:%S")
        handler.setFormatter(formatter)

        # add the handlers to the logger
        self.logger.addHandler(handler)


################################################################################
# EUDAT accounting Class #
################################################################################


class EUDATAccounting(object):
    """
    Class implementing the computation of statistics about resource consumption.
    """

    def __init__(self, conf, logger):
        """
        Initialize object with configuration parameters.
        """
        self.conf = conf
        self.logger = logger
        self.b2share_accounting = B2SHAREAccounting(conf, logger)

    def _toAccountingRecord(self, stats):
        """
        Cast to format of an eudat accounting record
        """
        return {
            'account': self.conf.account,
            'number': stats[0],
            'value': stats[1],
        }

    def reportStatistics(self, args):
        """
        Report statistical data on resource consumption to remote server
        """
        data = self.b2share_accounting.report()

        acctRecords = []
        acctRecords.append(self._toAccountingRecord(data))
        # adding the data to the args so other command line args
        # resp their defaults are available as well
        args.account = acctRecords[0]['account']
        args.value = acctRecords[0]['value']
        args.number = acctRecords[0]['number']
        pretty_data = json.dumps(acctRecords, indent=4)
        self.logger.info('Data: ' + pretty_data)

        credentials = utils.getCredentials(self.conf)
        self.logger.info("Credentials found")
        self.logger.debug("Credentials: " + str(credentials))
        url = utils.getUrl(self.conf)
        self.logger.info("URL to call: " + url)
        data = utils.getData(args)
        self.logger.info("Data as query string: " + data)

        if args.test:
            print("Test: Would send the following data: " \
                + data)
            return None

        response = utils.call(credentials, url, data)

        self.logger.info('Data sent. Status code: ' \
                         + str(response.status_code))
        if args.verbose:
            print("\nData sent. Status code: " \
                + str(response.status_code))
            print("Key of generated accounting record: " \
                + response.text)


def main(argv=sys.argv):
    logging.basicConfig(filename='.accounting.log',
                        level=logging.INFO,
                        format='%(asctime)s - %(name)s \
                        - %(levelname)s - %(message)s')
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
    """
    The main Application class of the B2SHARE collector

    :param argv: The command line as a list as ``sys.argv``
    """

    def __init__(self, argv):
        ap = argparse.ArgumentParser()
        ap.add_argument('--version', action='version', version=__version__)

        ap.add_argument('-c', '--configpath', default='./b2sharecollector.cfg',
                        help='path to configuration file. ' \
                             'Default: "./b2sharecollector.cfg" (in the current working directory)')

        utils.addCommonArguments(ap)

        self.args = ap.parse_args(args=argv[1:])
        # sneak in some default values that the utility functions expect
        self.args.unit = 'byte'
        self.args.service = '(default)'  # XXX TODO: should this come from the config?
        self.args.object_type = 'registered object'
        """Arguments of your app"""

    def run(self):
        LOG.info("B2SHAREcollector called with: " + str(self.args))
        print("B2SHAREcollector called with: %s" % str(self.args))

        fileparser = SafeConfigParser()
        fileparser.read(self.args.configpath)

        logger = logging.getLogger('StorageAccounting')
        logger.setLevel(logging.INFO)

        configuration = Configuration(self.args.configpath,
                                      logger, fileparser)
        configuration.parseConf()

        eurep = EUDATAccounting(configuration, logger)
        logger.info("Accounting starting ...")
        eurep.reportStatistics(self.args)
        logger.info("Accounting finished")
