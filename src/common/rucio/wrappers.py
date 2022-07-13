import abc
import subprocess

from gfal2 import Gfal2Context
from rucio.client.accountclient import AccountClient
from rucio.client.client import Client
from rucio.client.rseclient import RSEClient
from rucio.client.downloadclient import DownloadClient
from rucio.client.pingclient import PingClient
from rucio.client.uploadclient import UploadClient
from rucio.common.exception import AccountNotFound, RucioException


class RucioWrappers:
    """ Client wrappers for common functionality. """
    def __init__(self):
        pass

    @abc.abstractstaticmethod
    def addAccount():
        raise NotImplementedError

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
    def getAccount(account):
        raise NotImplementedError

    @abc.abstractstaticmethod
    def getMetadata():
        raise NotImplementedError

    @abc.abstractstaticmethod
    def setMetadata():
        raise NotImplementedError

    @abc.abstractstaticmethod
    def setMetadataBulk():
        raise NotImplementedError

    @abc.abstractstaticmethod
    def getRequestHistory():
        raise NotImplementedError

    @abc.abstractstaticmethod
    def getRSELimits(rse):
        raise NotImplementedError()

    @abc.abstractstaticmethod
    def getRSEProtocols(rse):
        raise NotImplementedError()

    @abc.abstractstaticmethod
    def getRSEUsage(rse):
        raise NotImplementedError()

    @abc.abstractstaticmethod
    def listAccounts():
        raise NotImplementedError

    @abc.abstractstaticmethod
    def listContent():
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
    def listRequests():
        raise NotImplementedError

    @abc.abstractstaticmethod
    def listRequestsHistory():
        raise NotImplementedError

    @abc.abstractstaticmethod
    def listRSEs():
        raise NotImplementedError()

    @abc.abstractstaticmethod
    def listRSEAttributes(rse):
        raise NotImplementedError()

    @abc.abstractstaticmethod
    def ping():
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
    """ Talk to a Rucio instance via subprocessed CLI commands. """

    @staticmethod
    def addDataset(did):
        rtn = subprocess.run(["rucio", "add-dataset", did], stdout=subprocess.PIPE)
        if rtn.returncode != 0:
            raise Exception("Non-zero return code")
        return rtn

    @staticmethod
    def addRule(did, copies, dst, lifetime=None, activity=None, src=None):
        """
        Add rule(s) for <copies> copies of a DID, <did>, at RSE, <rse>, with
        additional options.
        """
        cmd = ["rucio", "add-rule"]
        if lifetime:
            cmd.extend(["--lifetime", str(lifetime)])
        if activity:
            cmd.extend(["--activity", activity])
        if src:
            cmd.extend(["--source-replica-expression", src])
        cmd.extend([did, str(copies), dst])

        rtn = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
        )
        if rtn.returncode != 0:
            raise Exception("Non-zero return code")
        return rtn

    @staticmethod
    def attach(todid, dids):
        """ Attach DIDs, <dids>, to DID <todid>. """
        rtn = subprocess.run(["rucio", "attach", todid, dids], stdout=subprocess.PIPE)
        if rtn.returncode != 0:
            raise Exception("Non-zero return code")
        return rtn

    @staticmethod
    def listDIDs(scope, name="*", filters=None):
        """ List DIDs in scope, <scope>, with name, <name>. """
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
    def getMetadata(did, plugin="DID_COLUMN"):
        """
        Get metadata for did, <did>.

        Args:
            plugin (`str`): can be DID_COLUMN or JSON
        """
        rtn = subprocess.run(
            [
                "rucio",
                "get-metadata",
                did,
                "--plugin",
                plugin,
            ],
            stdout=subprocess.PIPE,
        )
        if rtn.returncode != 0:
            raise Exception("Non-zero return code")
        return rtn

    @staticmethod
    def setMetadata(did, key, value):
        """ Set metadata for did, <did>. """
        rtn = subprocess.run(
            [
                "rucio",
                "set-metadata",
                "--did",
                did,
                "--key",
                key,
                "--value",
                value,
            ],
            stdout=subprocess.PIPE,
        )
        if rtn.returncode != 0:
            raise Exception("Non-zero return code")
        return rtn


    @staticmethod
    def upload(rse, scope, filePath, lifetime):
        """ Upload file, <filePath>, to rse, <RSE>, with lifetime <lifetime>. """
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
        Upload a directory of files, <dirPath>, with lifetime, <lifetime>, and
        attach each file to a parent did, <parentDid>.
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
    """ Talk to a Rucio instance via the API. """

    @abc.abstractstaticmethod
    def addAccount(name, type, email):
        """ Create a new account. Types are 'USER', 'GROUP' or 'SERVICE'. """
        try:
            client = AccountClient()
            return client.add_account(account=name, type_=type, email=email)
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def addDID(did, type):
        """ Add a DID, <did>, of type, <type>. """
        try:
            client = Client()
            tokens = did.split(":")
            scope = tokens[0]
            name = tokens[1]
            client.add_did(scope=scope, name=name, did_type=type)
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def addReplica(rse, did, pfn):
        """ Add a DID, <did>, of type, <type>. """
        try:
            client = Client()
            gfal = Gfal2Context()
            tokens = did.split(":")
            scope = tokens[0]
            name = tokens[1]
            size = gfal.stat(pfn).st_size
            checksum = gfal.checksum(pfn, "adler32")
            client.add_replica(
                rse=rse, scope=scope, name=name, bytes=size, adler32=checksum, pfn=pfn
            )
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def addRule(
        did, copies, dst, lifetime, activity=None, src=None, asynchronous=False
    ):
        """
        Add rule(s) for <copies> copies of a DID, <did>, at RSE, <rse>, with
        additional options.
        """
        tokens = did.split(":")
        scope = tokens[0]
        name = tokens[1]

        client = Client()
        rtn = client.add_replication_rule(
            dids=[{"scope": scope, "name": name}],
            copies=copies,
            rse_expression=dst,
            lifetime=lifetime,
            activity=activity,
            source_replica_expression=src,
            asynchronous=asynchronous,
        )
        return rtn

    @staticmethod
    def attach(todid, dids):
        """ Attach DIDs, <dids>, to a DID, <todid>. """
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

    @staticmethod
    def detach(fromdid, dids):
        """ Attach DIDs, <dids>, from a DID, <fromdid>. """
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

    @staticmethod
    def download(did, baseDir="download", logger=None):
        """ Download a DID, <did>, to directory, <baseDir>. """
        client = DownloadClient(logger=logger)
        items = [{"did": did, "base_dir": baseDir}]
        client.download_dids(items=items)

    @staticmethod
    def erase(did, purgeReplicas):
        """ Remove replication rueles from a DID, <did>. """
        api = RucioWrappersAPI()
        rules = api.listReplicationRules(did)
        rids = set()
        for rule in rules:
            rids.add(rule["id"])
        client = Client()
        for rid in rids:
            client.delete_replication_rule(rule_id=rid, purge_replicas=purgeReplicas)

    @abc.abstractstaticmethod
    def getAccount(name):
        """ Returns a dictionary with information about an account. """
        try:
            client = AccountClient()
            return client.get_account(name)
        except AccountNotFound:
            return None
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def getMetadata(did, plugin="DID_COLUMN"):
        """
        Get DID metadata for did, <did>.

        Args:
            plugin (`str`): can be DID_COLUMN or JSON
        """
        client = Client()
        tokens = did.split(":")
        scope = tokens[0]
        name = tokens[1]
        metadata = client.get_metadata(scope, name, plugin=plugin)
        return metadata

    @staticmethod
    def setMetadata(did, key, value, recursive=False):
        """ Set DID metadata for did, <did>. """
        client = Client()
        tokens = did.split(":")
        scope = tokens[0]
        name = tokens[1]
        client.set_metadata(scope, name, key, value, recursive=recursive)

    @staticmethod
    def setMetadataBulk(did, meta, recursive=False):
        """
        Set bulk DID metadata for did, <did>.

        Args:
            meta (`dict`): Metadata k-v pairs to be set
        """
        client = Client()
        tokens = did.split(":")
        scope = tokens[0]
        name = tokens[1]
        client.set_metadata_bulk(scope, name, meta, recursive=recursive)

    @staticmethod
    def getRequestHistory(did, rse):
        """ Getfull history of requests for a DID, <did>, to rse, <rse>. """
        client = Client()
        tokens = did.split(":")
        scope = tokens[0]
        name = tokens[1]
        requests = client.list_request_history_by_did(scope=scope, name=name, rse=rse)
        return requests

    @staticmethod
    def getRSELimits(rse):
        """ Get RSE limits for rse, <rse>. """
        client = RSEClient()
        limits = client.get_rse_limits(rse)
        return limits

    @staticmethod
    def getRSEProtocols(rse):
        """ Get supported RSE protocols for rse, <rse>. """
        client = RSEClient()
        protocols = client.get_protocols(rse)
        return protocols

    @staticmethod
    def getRSEUsage(rse):
        """ Get RSE usage for rse, <rse>. """
        client = RSEClient()
        usage = client.get_rse_usage(rse)
        return usage

    @staticmethod
    def listAccounts():
        """ Returns a dictionary with accounts information. """
        try:
            client = AccountClient()
            return client.list_accounts()
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def listContent(scope, name):
        """ List content of DID. """
        client = Client()
        contents = []
        for content in client.list_content(scope=scope, name=name):
            contents.append(content)
        return contents

    @staticmethod
    def listDIDs(scope, filters=None, type="collection", recursive=False):
        """ List DIDs in scope, <scope>, with name, <name>. """
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
        for name in client.list_dids(scope=scope, filters=filters_dict, did_type=type, recursive=recursive):
            dids.append("{}:{}".format(scope, name))
        return dids

    @staticmethod
    def listFileReplicas(did, rse=None):
        """ List file replicas for DID, <did>. """
        client = Client()
        tokens = did.split(":")
        scope = tokens[0]
        name = tokens[1]
        replicas = []
        for replica in client.list_replicas(
            dids=[{"scope": scope, "name": name}], rse_expression=rse
        ):
            replicas.append(replica)
        return replicas

    @staticmethod
    def listReplicationRules(did):
        """ List replication rules for a DID, <did>. """
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

    @staticmethod
    def listReplicationRulesFull(did):
        """ List full history of replication rules for a DID, <did>. """
        client = Client()
        tokens = did.split(":")
        scope = tokens[0]
        name = tokens[1]
        rules = []
        for rule in client.list_replication_rule_full_history(scope=scope, name=name):
            rules.append(rule)
        return rules
        
    @staticmethod
    def listRequests(src_rse, dst_rse, request_states):
        """ List requests. """
        client = Client()
        requests = []
        for request in client.list_requests(
            src_rse=src_rse, dst_rse=dst_rse, request_states=','.join(request_states)):
                requests.append(request)
        return requests

    @staticmethod
    def listRequestsHistory(src_rse, dst_rse, request_states):
        """ List requests history. """
        client = Client()
        requests = []
        for request in client.list_requests_history(
            src_rse=src_rse, dst_rse=dst_rse, request_states=','.join(request_states)):
                requests.append(request)
        return requests

    @staticmethod
    def listRSEs(rse=None):
        """ List RSEs. """
        client = Client()
        rses = client.list_rses(rse)
        return rses

    @staticmethod
    def listRSEAttributes(rse):
        """ List RSE attributes for RSE, <rse>. """
        client = Client()
        rseDict = client.list_rse_attributes(rse)
        return rseDict

    @staticmethod
    def ruleInfo(ruleId):
        """ Get replication rule information for rule id, <ruleId>. """
        client = Client()
        info = client.get_replication_rule(ruleId)
        return info

    @staticmethod
    def ping():
        """ Ping a rucio server. """
        client = PingClient()
        return client.ping()

    @staticmethod
    def upload(
        rse,
        scope,
        filePath,
        lifetime,
        registerAfterUpload=True,
        forceScheme=None,
        transferTimeout=30,
        logger=None,
    ):
        """ Upload file, <filePath>, to rse, <RSE>, with lifetime <lifetime>. """
        items = []
        items.append(
            {
                "path": filePath,
                "rse": rse,
                "did_scope": scope,
                "lifetime": lifetime,
                "register_after_upload": registerAfterUpload,
                "force_scheme": forceScheme,
                "transfer_timeout": transferTimeout,
            }
        )
        client = UploadClient(logger=logger)
        client.upload(items=items)

    @staticmethod
    def whoAmI():
        client = Client()
        return client.whoami()


