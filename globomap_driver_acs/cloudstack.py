# -*- coding: utf-8 -*-
# By: Kelcey Damage, 2012 & Kraig Amador, 2012
import hashlib
import hmac
import base64
import urllib
import json
import sys
import ssl


class SignedAPICall(object):

    def __init__(self, api_url, apiKey, secret, verifysslcert):
        self.api_url = api_url
        self.apiKey = apiKey
        self.secret = secret
        self.verifysslcert = verifysslcert

    def request(self, args, action):
        args['apiKey'] = self.apiKey

        self.params = []
        self._sort_request(args)
        self._create_signature()
        self._build_post_request(action)

    def _sort_request(self, args):
        keys = sorted(args.keys())
        for key in keys:
            self.params.append(key + '=' + urllib.quote_plus(args[key]))

    def _create_signature(self):
        self.query = '&'.join(self.params).replace("+", "%20")\
            .replace(":", "%3A")
        digest = hmac.new(
            self.secret,
            msg=self.query.lower(),
            digestmod=hashlib.sha1).digest()

        self.signature = base64.b64encode(digest)

    def _build_post_request(self, action='GET'):
        self.query += '&signature=' + urllib.quote_plus(self.signature)
        self.value = self.api_url
        if action == 'GET':
            self.value += '?' + self.query


class CloudStackClient(SignedAPICall):

    def __getattr__(self, name):
        def handlerFunction(*args, **kwargs):
            args = list(args)
            if len(args) == 1:
                args.insert(0, 'GET')
            action = args[0] or 'GET'
            if kwargs:
                return self._make_request(name, kwargs)
            return self._make_request(name, args[1], action)
        return handlerFunction

    def _http_get(self, url):
        if self.verifysslcert and sys.version_info < (2, 7, 9):
            response = urllib.urlopen(url)
        else:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

            response = urllib.urlopen(url, context=ctx)
        return response.read()

    def _http_post(self, url, data):
        if self.verifysslcert and sys.version_info < (2, 7, 9):
            response = urllib.urlopen(url, data)
        else:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            response = urllib.urlopen(url, data, context=ctx)
        return response.read()

    def _make_request(self, command, args, action='GET'):
        args['response'] = 'json'
        args['command'] = command
        self.request(args, action)

        tries = 3
        while True:
            try:
                if action == 'GET':
                    data = self._http_get(self.value)
                else:
                    data = self._http_post(self.value, self.query)
                break
            except IOError, e:
                tries -= 1
                if not tries:
                    raise e

        key = command.lower() + "response"
        if key == 'deletenetworkinglobonetworkresponse':
            key = 'deletenetworkresponse'
        if key == 'listglobonetworkpoolsresponse':
            key = 'listglobonetworkpoolresponse'
        if key == 'acquirenewlbipresponse':
            key = 'associateipaddressresponse'
        if key == 'listcountersresponse':
            key = 'counterresponse'
        if key == 'createconditionresponse':
            key = 'conditionresponse'
        if key == 'createautoscalepolicyresponse':
            key = 'autoscalepolicyresponse'
        if key == 'createautoscalevmprofileresponse':
            key = 'autoscalevmprofileresponse'
        if key == 'createautoscalevmgroupresponse':
            key = 'autoscalevmgroupresponse'
        if key == 'enableautoscalevmgroupresponse':
            key = 'enableautoscalevmGroupresponse'

        return json.loads(data)[key]


class CloudstackService(object):

    def __init__(self, cloudstack_client):
        self.cloudstack_client = cloudstack_client

    def get_virtual_machine(self, id):
        virtual_machines = self.cloudstack_client.\
            listVirtualMachines({'id': id, 'listall': 'true'})
        if virtual_machines['count'] == 1:
            return virtual_machines['virtualmachine'][0]

    def get_project(self, id):
        projects = self.cloudstack_client.\
            listProjects({'id': id, 'listall': 'true'})
        if projects['count'] == 1:
            return projects['project'][0]
