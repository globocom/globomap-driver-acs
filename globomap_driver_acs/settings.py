import os

'''
Used environment variables

ACS_$env_API_URL
ACS_$env__API_KEY
ACS_$env__API_SECRET_KEY
ACS_$env__RMQ_USER
ACS_$env__RMQ_PASSWORD
ACS_$env__RMQ_HOST
ACS_$env__RMQ_PORT
ACS_$env__RMQ_QUEUE
ACS_$env__RMQ_VIRTUAL_HOST
'''


def get_setting(env, key, default=None):
    value = os.getenv("ACS_%s_%s" % (env, key))
    if not value and default:
        return default
    return value
