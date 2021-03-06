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

"""
Controls if draft records (i.e. records that are not published) should
be included in used storage calculation.
"""
INCLUDE_DRAFT_RECORDS = True

class B2SHAREAccounting(object):

    def __init__(self, conf, logger):
        self.logger = logger
        self.url = conf.b2share_url
        self.community = conf.b2share_community
        self.api_token = conf.api_token
        self.page_size = PAGE_SIZE
        self.drafts_included = INCLUDE_DRAFT_RECORDS

    def _create_search_url(self):
        """Creates search url to query for records from B2SHARE REST API."""
        url = '{url}/api/records/?' \
              'q=community:{community}&size={page_size}' \
              .format(
                  url=self.url,
                  community=self.community,
                  page_size=self.page_size)

        # Add an access_token if one is provided
        if self.api_token:
            url = url + '&access_token={}'.format(self.api_token)

            # Should draft records be include in search.
            # NOTE: There is no point in searching 
            #       for drafts without API Token.
            #       They won't be listed.
            if self.drafts_included:
                url = url + '&drafts=1'
        return url

    def _calculate_storage_for_draft(self, record):
        # 1. Fetch links.self to get url of bucket.
        # 2. Fetch links.files which contains the url of bucket.
        # 3. Check value of 'size'-key to get bucket size.

        record_size = 0

        if record['links'].get('self'):
            r = requests.get(record['links']['self'] + '?access_token=' + self.api_token, verify=True)
            # Check that 200 OK was given
            # (i.e. access token contains enough permissions)
            if r.status_code == requests.codes.ok:
                reply = r.json()
                if reply['links'].get('files'):
                    r = requests.get(reply['links']['files'] + '?access_token=' + self.api_token, verify=True)
                    if r.status_code == requests.codes.ok:
                        reply = r.json()
                        record_size = reply.get('size')

        return record_size

    def _calculate_storage_for_record(self, record):
        record_size = 0

        if record['links'].get('publication'):
            r = requests.get(record['links']['publication'] + '?access_token=' + self.api_token, verify=True)
            # Check that 200 OK was given
            # (i.e. access token contains enough permissions)
            if r.status_code == requests.codes.ok:
                reply = r.json()
                if reply['links'].get('files'):
                    r = requests.get(reply['links']['files'] + '?access_token=' + self.api_token, verify=True)
                    if r.status_code == requests.codes.ok:
                        reply = r.json()
                        record_size = reply.get('size')
        return record_size

    def report(self, args):
        """ Get used storage space for community by querying B2SHARE REST API.

        Example of response from B2SHARE 'api/records/?q=community:e1800bc8-780e-4617-a7b6-2312cb6190c4'
        NOTE: - B2SHARE automatically return only 10 results per page, 
                if no 'size' parameter is used in the search.
              - No 'access_token' is included in this search.
              - This search doesn't include draft records.
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

        url = self._create_search_url()

        total_amount = 0
        total_hits = 0
        total_pages = 0

        try:

            # Check that api_token is valid.
            # NOTE: Check won't guarantee that api_token has superadmin rights.
            if self.api_token:
                token_check_url = '{url}/api/user/?{token}'.format(url=self.url, token=self.api_token)
                token_check_response = requests.get(token_check_url, verify=True)
                if token_check_response.json() == '{}':
                    # Since nothing was returned token is considered to be invalid.
                    raise requests.exceptions.RequestException('Provide API token is not valid.')
            r = requests.get(url, verify=True)

            while r:
                if r.status_code != requests.codes.ok:
                    self.logger.warn(
                        'get community records status code:' + r.status_code)

                reply = r.json()

                total_hits = reply['hits']['total']
                total_pages += 1

                for record in reply['hits']['hits']:
                    # Check if record is actually a draft.
                    if record['metadata']['publication_state'] == 'draft':
                        record_size = self._calculate_storage_for_draft(record)
                    else:
                        # Record has been published
                        record_size = self._calculate_storage_for_record(record)
                    
                    total_amount += record_size

                # Continue if there are multiple pages of search results.
                if r.links.get('next'):
                    next_url = r.links['next']['url']

                    # NOTE: Due to bug in B2SHARE REST API
                    #       '&access_token' and '&drafts=1' query params
                    #       need to be added manually
                    if self.drafts_included:
                        next_url = next_url + '&access_token={}&drafts=1'.format(self.api_token)
                    r = requests.get(next_url, verify=True)
                else:
                    r = False

        except requests.exceptions.RequestException as e:
            self.logger.error('get community records request failed:' + str(e))

        self.logger.debug(
            'get community records request contained {} pages.'.format(
                total_pages))
        return (total_hits, total_amount)
