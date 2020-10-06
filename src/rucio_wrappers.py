import abc
import os
import subprocess

import gfal2
from rucio.client.client import Client
from rucio.client.uploadclient import UploadClient
from rucio.common.exception import *

class RucioWrappers():
    @abc.abstractstaticmethod
    def addDataset(did):
        raise NotImplementedError


    @abc.abstractstaticmethod
    def addRule(did, copies, rse, lifetime):
        raise NotImplementedError


    @abc.abstractstaticmethod
    def attach(todid, dids):
        raise NotImplementedError


    @abc.abstractstaticmethod
    def listDIDs(scope, name="*", filt=None):
        raise NotImplementedError
              

    @abc.abstractstaticmethod
    def upload(rse, scope, filePath, lifetime):
        raise NotImplementedError


    @abc.abstractstaticmethod
    def uploadDir(rse, scope, dirPath, lifetime, parentDid):
        raise NotImplementedError


class RucioWrappersCLI(RucioWrappers):
    @staticmethod
    def addDataset(did):
        rtn = subprocess.run(
            ['rucio', 'add-dataset', did],
            stdout=subprocess.PIPE)
        if rtn.returncode != 0:
            raise Exception("Non-zero return code")
        return rtn


    @staticmethod
    def addRule(did, copies, rse, lifetime):
        rtn = subprocess.run(
            ['rucio', 'add-rule', '--lifetime', str(lifetime),
            did, str(copies), rse],
            stdout=subprocess.PIPE)
        if rtn.returncode != 0:
            raise Exception("Non-zero return code")
        return rtn


    @staticmethod
    def attach(todid, dids):
        rtn = subprocess.run(
            ['rucio', 'attach', todid, dids],
            stdout=subprocess.PIPE)
        if rtn.returncode != 0:
            raise Exception("Non-zero return code")
        return rtn


    @staticmethod
    def listDIDs(scope, name="*", filt=None):
        if filt is None:
            rtn = subprocess.run(
                ['rucio', 'list-dids', '{}:{}'.format(scope, name), '--short'],
                stdout=subprocess.PIPE)
        else:
            rtn = subprocess.run(
                ['rucio', 'list-dids', '{}:{}'.format(scope, name), '--short',
                '--filter', filt],
                stdout=subprocess.PIPE)
        if rtn.returncode != 0:
            raise Exception("Non-zero return code")

        rtn = rtn.stdout.decode('UTF-8')
        dids = []
        for did in rtn.split('\n'):
            if did is not '':
                dids.append(did)
        return dids


    @staticmethod
    def upload(rse, scope, filePath, lifetime):
        rtn = subprocess.run(
            ['rucio', 'upload', '--rse', rse, '--scope', scope,
            '--lifetime', str(lifetime), filePath],
            stdout=subprocess.PIPE)
        if rtn.returncode != 0:
            raise Exception("Non-zero return code")
        return rtn


    @staticmethod
    def uploadDir(rse, scope, dirPath, lifetime, parentDid):
        '''
        Upload a directory of files and attach each file to the parentDid
        '''
        rtn = subprocess.run(
            ['rucio', 'upload', '--rse', rse, '--lifetime', str(lifetime),
             '--scope', scope, parentDid, dirPath],
            stdout=subprocess.PIPE)
        if rtn.returncode != 0:
            raise Exception("Non-zero return code")
        return rtn


class RucioWrappersAPI(RucioWrappers):
    @staticmethod
    def addDataset(did):
        try:
            client = Client()
            tokens = did.split(':')
            scope = tokens[0]; name = tokens[1]
            client.add_dataset(scope=scope, name=name)
        except RucioException as error:
            raise Exception(error)


    @staticmethod
    def addRule(did, copies, rse, lifetime):
        tokens = did.split(':')
        scope = tokens[0]; name = tokens[1]
        try:
            client = Client()
            client.add_replication_rule(
                [{'scope': scope, 'name': name}], copies, rse, lifetime=lifetime)
        except RucioException as error:
            raise Exception(error)


    @staticmethod
    def attach(todid, dids):
        try:
            client = Client()
            tokens = todid.split(':')
            toscope = tokens[0]; toname = tokens[1]

            attachment = {
                'scope': toscope,
                'name': toname,
                'dids': []
            }
            for did in dids.split(' '):
                tokens = did.split(':')
                scope = tokens[0]; name = tokens[1]    
                attachment['dids'].append({
                    'scope': scope,
                    'name': name
                })
            client.attach_dids_to_dids([attachment])
        except RucioException as error:
            raise Exception(error)


    @staticmethod
    def listDIDs(scope, name="*", filt=None):
        try:
            client = Client()
            filt_dict = {}
            if filt is not None:
                for pair in filt.split(','):
                    tokens = pair.split('=')
                    key = tokens[0]; val = tokens[1]
                    filt_dict[key] = val
            dids = []
            for did in client.list_dids(scope=scope, filters=filt_dict):
                dids.append(did)
            return dids
        except RucioException as error:
            raise Exception(error)


    @staticmethod
    def upload(rse, scope, filePath, lifetime):
        items = []
        items.append({
            'path': filePath,
            'rse': rse,
            'did_scope': scope,
            'lifetime': lifetime
        })
        try:
            client = UploadClient()
            client.upload(items)
        except RucioException as error:
            raise Exception(error)
