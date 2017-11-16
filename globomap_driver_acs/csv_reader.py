"""
   Copyright 2017 Globo.com

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
import csv
import logging
import requests


class CsvReader(object):

    log = logging.getLogger(__name__)

    def __init__(self, file_url, delimiter, ignore_header=False):
        self.file_url = file_url
        self.delimiter = delimiter
        self.ignore_header = ignore_header

    def get_lines(self):
        lines = []
        try:
            csv_file = self._get_file()
            if csv_file:
                parsed_lines = csv.reader(csv_file, delimiter=self.delimiter)
                if self.ignore_header:
                    next(parsed_lines)

                for line in parsed_lines:
                    lines.append(line)
            else:
                self.log.error("Unable to find file at %s" % self.file_url)
        except:
            self.log.exception("Error reading CSV file %s" % self.file_url)
            raise

        return iter(lines)

    def _get_file(self):
        if not self.file_url:
            return
        response = requests.get(self.file_url)
        if response.status_code == 200:
            return response.content.split('\n')
