import abc
import subprocess

from rucio.client.client import Client
from rucio.client.downloadclient import DownloadClient
from rucio.client.uploadclient import UploadClient
from rucio.common.exception import RucioException


class RucioWrappers:
    @abc.abstractstaticmethod
    def addDataset():
        raise NotImplementedError

    @abc.abstractstaticmethod
    def addRule():
        raise NotImplementedError

    @abc.abstractstaticmethod
    def attach():
        raise NotImplementedError

    @abc.abstractstaticmethod
    def detach():
        raise NotImplementedError

    @abc.abstractstaticmethod
    def download():
        raise NotImplementedError

    @abc.abstractstaticmethod
    def erase():
        raise NotImplementedError

    @abc.abstractstaticmethod
    def getMetadata():
        raise NotImplementedError

    @abc.abstractstaticmethod
    def listDIDs():
        raise NotImplementedError

    @abc.abstractstaticmethod
    def listFileReplicas():
        raise NotImplementedError

    @abc.abstractstaticmethod
    def listReplicationRules():
        raise NotImplementedError

    @abc.abstractstaticmethod
    def listReplicationRulesFull():
        raise NotImplementedError

    @abc.abstractstaticmethod
    def listRSEs():
        raise NotImplementedError()

    @abc.abstractstaticmethod
    def listRSEAttributes(rse):
        raise NotImplementedError()

    @abc.abstractstaticmethod
    def upload():
        raise NotImplementedError

    @abc.abstractstaticmethod
    def uploadDir():
        raise NotImplementedError

    @abc.abstractstaticmethod
    def whoAmI():
        raise NotImplementedError


class RucioWrappersCLI(RucioWrappers):
    @staticmethod
    def addDataset(did):
        rtn = subprocess.run(["rucio", "add-dataset", did], stdout=subprocess.PIPE)
        if rtn.returncode != 0:
            raise Exception("Non-zero return code")
        return rtn

    @staticmethod
    def addRule(did, copies, rse, lifetime):
        rtn = subprocess.run(
            ["rucio", "add-rule", "--lifetime", str(lifetime), did, str(copies), rse],
            stdout=subprocess.PIPE,
        )
        if rtn.returncode != 0:
            raise Exception("Non-zero return code")
        return rtn

    @staticmethod
    def addRuleWithOptions(
        did, copies, dst, lifetime, activity=None, src=None
    ):
        rtn = subprocess.run(
            [
                "rucio",
                "-v",
                "add-rule",
                "--lifetime",
                str(lifetime),
                "--activity",
                activity,
                "--source-replica-expression",
                src,
                did,
                str(copies),
                dst,
            ],
            stdout=subprocess.PIPE,
        )
        if rtn.returncode != 0:
            raise Exception("Non-zero return code")
        return rtn

    @staticmethod
    def attach(todid, dids):
        rtn = subprocess.run(["rucio", "attach", todid, dids], stdout=subprocess.PIPE)
        if rtn.returncode != 0:
            raise Exception("Non-zero return code")
        return rtn

    @staticmethod
    def listDIDs(scope, name="*", filters=None):
        if filters is None:
            rtn = subprocess.run(
                ["rucio", "list-dids", "{}:{}".format(scope, name), "--short"],
                stdout=subprocess.PIPE,
            )
        else:
            rtn = subprocess.run(
                [
                    "rucio",
                    "list-dids",
                    "{}:{}".format(scope, name),
                    "--short",
                    "--filter",
                    filters,
                ],
                stdout=subprocess.PIPE,
            )
        if rtn.returncode != 0:
            raise Exception("Non-zero return code")

        rtn = rtn.stdout.decode("UTF-8")
        dids = []
        for did in rtn.split("\n"):
            if did != "":
                dids.append(did)
        return dids

    @staticmethod
    def upload(rse, scope, filePath, lifetime):
        rtn = subprocess.run(
            [
                "rucio",
                "upload",
                "--rse",
                rse,
                "--scope",
                scope,
                "--lifetime",
                str(lifetime),
                "--register-after-upload",
                filePath,
            ],
            stdout=subprocess.PIPE,
        )
        if rtn.returncode != 0:
            raise Exception("Non-zero return code")
        return rtn

    @staticmethod
    def uploadDir(rse, scope, dirPath, lifetime, parentDid):
        """
        Upload a directory of files and attach each file to the parentDid
        """
        rtn = subprocess.run(
            [
                "rucio",
                "upload",
                "--rse",
                rse,
                "--lifetime",
                str(lifetime),
                "--scope",
                scope,
                "--register-after-upload",
                parentDid,
                dirPath,
            ],
            stdout=subprocess.PIPE,
        )
        if rtn.returncode != 0:
            raise Exception("Non-zero return code")
        return rtn


class RucioWrappersAPI(RucioWrappers):
    @staticmethod
    def addDataset(did):
        try:
            client = Client()
            tokens = did.split(":")
            scope = tokens[0]
            name = tokens[1]
            client.add_dataset(scope=scope, name=name)
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def addRule(
        did, copies, dst, lifetime, activity=None, src=None
    ):
        tokens = did.split(":")
        scope = tokens[0]
        name = tokens[1]
        try:
            client = Client()
            rtn = client.add_replication_rule(
                dids=[{"scope": scope, "name": name}],
                copies=copies,
                rse_expression=dst,
                lifetime=lifetime,
                activity=activity,
                source_replica_expression=src,
            )
            return rtn
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def attach(todid, dids):
        try:
            client = Client()
            tokens = todid.split(":")
            toScope = tokens[0]
            toName = tokens[1]

            attachment = {"scope": toScope, "name": toName, "dids": []}
            for did in dids.split(" "):
                tokens = did.split(":")
                scope = tokens[0]
                name = tokens[1]
                attachment["dids"].append({"scope": scope, "name": name})
            client.attach_dids_to_dids(attachments=[attachment])
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def detach(fromdid, dids):
        try:
            client = Client()
            tokens = fromdid.split(":")
            fromScope = tokens[0]
            fromName = tokens[1]

            detachments = []
            for did in dids.split(" "):
                tokens = did.split(":")
                scope = tokens[0]
                name = tokens[1]
                detachments.append({"scope": scope, "name": name})
            client.detach_dids(scope=fromScope, name=fromName, dids=detachments)
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def download(did, baseDir="download", logger=None):
        try:
            client = DownloadClient(logger=logger)
            items = [{"did": did, "base_dir": baseDir}]
            client.download_dids(items=items)
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def erase(did):
        try:
            api = RucioWrappersAPI()
            rules = api.listReplicationRules(did)
            rids = set()
            for rule in rules:
                rids.add(rule["id"])
            client = Client()
            for rid in rids:
                client.delete_replication_rule(rule_id=rid, purge_replicas=True)
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def getMetadata(did):
        try:
            client = Client()
            tokens = did.split(":")
            scope = tokens[0]
            name = tokens[1]
            metadata = client.get_metadata(scope, name)
            return metadata
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def listDIDs(scope, name="*", filters=None):
        try:
            client = Client()
            filters_dict = {}
            if filters is not None:
                if isinstance(filters, str):
                    for pair in filters.split(","):
                        tokens = pair.split("=")
                        key = tokens[0].strip()
                        val = tokens[1].strip()
                        filters_dict[key] = val
                else:
                    filters_dict = filters
            dids = []
            for name in client.list_dids(scope=scope, filters=filters_dict):
                dids.append('{}:{}'.format(scope, name))
            return dids
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def listFileReplicas(did, rse=None):
        try:
            client = Client()
            tokens = did.split(":")
            scope = tokens[0]
            name = tokens[1]
            replicas = []
            for replica in client.list_replicas(dids=[{"scope": scope, "name": name}],
            rse_expression=rse):
                replicas.append(replica)
            return replicas
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def listReplicationRules(did):
        try:
            client = Client()
            tokens = did.split(":")
            scope = tokens[0]
            name = tokens[1]
            rules = []
            for rule in client.list_replication_rules(
                filters={"scope": scope, "name": name}
            ):
                rules.append(rule)
            return rules
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def listReplicationRulesFull(did):
        try:
            client = Client()
            tokens = did.split(":")
            scope = tokens[0]
            name = tokens[1]
            rules = []
            for rule in client.list_replication_rule_full_history(
                scope=scope, name=name
            ):
                rules.append(rule)
            return rules
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def listRSEs(rse=None):
        try:
            client = Client()
            rses = client.list_rses(rse)
            return rses
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def listRSEAttributes(rse):
        try:
            client = Client()
            rseDict = client.list_rse_attributes(rse)
            return rseDict
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def ruleInfo(ruleID):
        try:
            client = Client()
            info = client.get_replication_rule(ruleID)
            return info
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def upload(rse, scope, filePath, lifetime, registerAfterUpload=True,
    forceScheme=None, transferTimeout=30, logger=None):
        items = []
        items.append(
            {
                "path": filePath,
                "rse": rse,
                "did_scope": scope,
                "lifetime": lifetime,
                "register_after_upload": True,
                "force_scheme": forceScheme,
                "transfer_timeout": transferTimeout
            }
        )
        try:
            client = UploadClient(logger=logger)
            client.upload(items=items)
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def whoAmI():
        try:
            client = Client()
            return client.whoami()
        except RucioException as error:
            raise Exception(error)
