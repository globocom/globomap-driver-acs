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

Used environment variables

ACS_$env_API_URL
ACS_$env_API_KEY
ACS_$env_API_SECRET_KEY
ACS_$env_RMQ_USER
ACS_$env_RMQ_PASSWORD
ACS_$env_RMQ_HOST
ACS_$env_RMQ_PORT
ACS_$env_RMQ_QUEUE
ACS_$env_RMQ_EXCHANGE
ACS_$env_RMQ_LOADER_EXCHANGE
ACS_$env_RMQ_VIRTUAL_HOST
"""
import os

DEFAULT_PROCESS_ID = os.getenv(
    'CUSTEIO_DEFAULT_PROCESS_ID', '7a9456320f328700fd7f91dbe1050e27')

GLOBOMAP_LOADER_API_URL = os.getenv('GLOBOMAP_LOADER_API_URL')
GLOBOMAP_LOADER_API_USERNAME = os.getenv('GLOBOMAP_LOADER_API_USERNAME')
GLOBOMAP_LOADER_API_PASSWORD = os.getenv('GLOBOMAP_LOADER_API_PASSWORD')


def get_setting(env, key, default=None):
    value = os.getenv('ACS_%s_%s' % (env, key))
    if not value and default:
        return default
    return value
