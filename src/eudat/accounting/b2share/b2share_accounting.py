# -*- coding: utf-8 -*-
#
# Copyright (c) 2018 CSC - IT Center for Science Ltd.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import requests


"""
Controls how many results one reply from B2SHARE can contain.
Don't try with value '0', this will loop indefinitely.
"""
PAGE_SIZE = '100'


class B2SHAREAccounting(object):

    def __init__(self, conf, logger):
        self.logger = logger
        self.url = conf.b2share_url
        self.community = conf.b2share_community

    def report(self, args):

        """
        Example of response from B2SHARE 'api/records/?q=community:e1800bc8-780e-4617-a7b6-2312cb6190c4'
        NOTE: B2SHARE automatically return only 10 results, if no 'size' parameter is used in the search
        {
        "aggregations": {
            "type": {
            "buckets": [],
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0
            }
        },
        "hits": {
            "hits": [
            {
                "created": "2018-06-05T12:33:11.228939+00:00",
                "files": [
                {
                    "bucket": "6389e7da-28a1-4546-b28c-59f727ec5dcc",
                    "checksum": "md5:24e0d8374584140f984b7fb1dd57422a",
                    "ePIC_PID": "http://hdl.handle.net/11304/708c3a4a-a765-40c0-8870-878503c73d17",
                    "key": "LR2_MIco_PSav_CTpy_VPco_SF0_RFpk_CB_TS5_TAal.shp",
                    "size": 419988,
                    "version_id": "c4fbcf7c-3550-4900-9197-4af90e847549"
                }
                ],
                ...
            },
            {
            "created": "2018-05-28T17:00:48.052714+00:00",
            "files": [
            {
                "bucket": "6e50c448-e35b-4891-87b5-bacedf2ac3a9",
                "checksum": "md5:b249f0bc1cdfccf7400cbb284b54c054",
                "ePIC_PID": "http://hdl.handle.net/11304/8e83c8f1-0544-4ab3-b44b-c16907b1426e",
                "key": "LR1_MItrh_PSav_SF0_CB_TS5_TAal_ITic15_humidity.tiff",
                "size": 1472498,
                "version_id": "93ed5aa2-2ae2-4711-851a-ffca72a2dbbe"
            }
            ],
            ...
        ],
        "total": 9129
        },
        "links": {
            "next": "https://b2share.eudat.eu/api/records/?size=100&sort=bestmatch&q=community%3Ae1800bc8-780e-4617-a7b6-2312cb6190c4&page=2",
            "self": "https://b2share.eudat.eu/api/records/?size=100&sort=bestmatch&q=community%3Ae1800bc8-780e-4617-a7b6-2312cb6190c4&page=1"
        }
        }
        """  # noqa

        url = '{url}/api/records/?' \
              'q=community:{community}&size={page_size}' \
              .format(
                  url=self.url,
                  community=self.community,
                  page_size=self.page_size)

        total_amount = 0
        total_hits = 0
        total_pages = 0

        try:
            r = requests.get(url, verify=True)

            while r:

                if r.status_code != requests.codes.ok:
                    self.logger.warn(
                        'get community records status code:' + r.status_code)

                reply = r.json()

                total_hits = reply['hits']['total']

                total_pages += 1

                for record in reply['hits']['hits']:
                    if 'files' in record:
                        for record_file in record['files']:
                            total_amount += record_file['size']

                if r.links.get('next'):
                    r = requests.get(r.links['next']['url'], verify=True)
                else:
                    r = False

        except requests.exceptions.RequestException as e:
            self.logger.error('get community records request failed:' + str(e))

        self.logger.debug(
            'get community records request contained {} pages.'.format(
                total_pages))

        return (total_hits, total_amount)
