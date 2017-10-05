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
        if virtual_machines.get('count') == 1:
            return virtual_machines['virtualmachine'][0]

    def list_virtual_machines_by_project(self, project_id):
        virtual_machines = self.cloudstack_client. \
            listVirtualMachines({'listall': 'true', 'projectid': project_id})
        if not virtual_machines.get('virtualmachine'):
            return []
        return virtual_machines['virtualmachine']

    def get_project(self, id):
        if id:
            projects = self.cloudstack_client.\
                listProjects({'id': id, 'listall': 'true'})
            if projects.get('count') == 1:
                return projects['project'][0]

    def list_projects(self):
        projects = self.cloudstack_client.\
            listProjects({'listall': 'true', 'simple': 'true'})
        return projects['project']
