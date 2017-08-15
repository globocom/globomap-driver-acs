import os

'''
Used environment variables

ACS_$env_API_URL
ACS_$env_API_KEY
ACS_$env_API_SECRET_KEY
ACS_$env_RMQ_USER
ACS_$env_RMQ_PASSWORD
ACS_$env_RMQ_HOST
ACS_$env_RMQ_PORT
ACS_$env_RMQ_QUEUE
ACS_$env_RMQ_LOADER_EXCHANGE
ACS_$env_RMQ_VIRTUAL_HOST
'''


def get_setting(env, key, default=None):
    value = os.getenv("ACS_%s_%s" % (env, key))
    if not value and default:
        return default
    return value
