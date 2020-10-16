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
    def get_metadata():
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
    def list_rses():
        raise NotImplementedError()

    @abc.abstractstaticmethod
    def list_rse_attributes(rse):
        raise NotImplementedError()

    @abc.abstractstaticmethod
    def upload():
        raise NotImplementedError

    @abc.abstractstaticmethod
    def uploadDir():
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
        did, copies, rse, lifetime, activity=None, source_rse_expr=None
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
                source_rse_expr,
                did,
                str(copies),
                rse,
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
    def addRule(did, copies, rse_expression, lifetime):
        tokens = did.split(":")
        scope = tokens[0]
        name = tokens[1]
        try:
            client = Client()
            client.add_replication_rule(
                dids=[{"scope": scope, "name": name}],
                copies=copies,
                rse_expression=rse_expression,
                lifetime=lifetime,
            )
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def addRuleWithOptions(
        did, copies, rse_expression, lifetime, activity=None, source_rse_expr=None
    ):
        tokens = did.split(":")
        scope = tokens[0]
        name = tokens[1]
        try:
            client = Client()
            client.add_replication_rule(
                dids=[{"scope": scope, "name": name}],
                copies=copies,
                rse_expression=rse_expression,
                lifetime=lifetime,
                activity=activity,
                source_replica_expression=source_rse_expr,
            )
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
    def download(did, base_dir="download"):
        try:
            client = DownloadClient()
            items = [{"did": did, "base_dir": base_dir}]
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
    def get_metadata(did):
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
    def list_rse_attributes(rse):
        try:
            client = Client()
            rse_dict = client.list_rse_attributes(rse)
            return rse_dict
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def list_rses(rse_expression=None):
        try:
            client = Client()
            rses = client.list_rses(rse_expression)
            return rses
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
            for did in client.list_dids(scope=scope, filters=filters_dict):
                dids.append(did)
            return dids
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def listFileReplicas(did):
        try:
            client = Client()
            tokens = did.split(":")
            scope = tokens[0]
            name = tokens[1]
            replicas = []
            for replica in client.list_replicas(dids=[{"scope": scope, "name": name}]):
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
    def upload(rse, scope, filePath, lifetime):
        items = []
        items.append(
            {"path": filePath, "rse": rse, "did_scope": scope, "lifetime": lifetime}
        )
        try:
            client = UploadClient()
            client.upload(items=items, register_after_upload=True)
        except RucioException as error:
            raise Exception(error)
