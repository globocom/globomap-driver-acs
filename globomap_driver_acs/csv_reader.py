import csv
import logging
import os


class CsvReader(object):

    log = logging.getLogger(__name__)

    def __init__(self, file_path, delimiter, ignore_header=False):
        self.file_path = file_path
        self.delimiter = delimiter
        self.ignore_header = ignore_header

    def get_lines(self):
        lines = []
        try:
            if self._file_exists(self.file_path):
                with open(self.file_path, 'r') as file:
                    parsed_lines = csv.reader(file, delimiter=self.delimiter)
                    if self.ignore_header:
                        next(parsed_lines)

                    for line in parsed_lines:
                        lines.append(line)
            else:
                self.log.error("Unable to find file at %s" % self.file_path)
        except:
            self.log.exception("Error reading CSV file %s" % self.file_path)
            raise

        return iter(lines)

    def _file_exists(self, file_path):
        return self.file_path and os.path.isfile(file_path)
