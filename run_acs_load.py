#!/usr/bin/env python
import logging
import sys
from globomap_driver_acs.load import CloudstackDataLoader

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s %(levelname)s %(message)s')
    CloudstackDataLoader(sys.argv[1]).run()
