import os
import subprocess

class Rucio():
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
    def attach(scope, todid, dids):
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