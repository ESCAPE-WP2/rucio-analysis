import os

from common.rucio.helpers import createCollection
from common.rucio.wrappers import RucioWrappersAPI
from common.rucio.admin_wrappers import RucioAdminWrappersAPI
from tasks.task import Task
from utility import generateRandomFile


class MetadataReplication(Task):
    """ Rucio API test class stub. """

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            # Assign variables from test.yml kwargs.
            #
            scope = kwargs["scope"]
            uploadTo = kwargs["upload_to"]
            size = kwargs["size"]
            lifetime = kwargs["lifetime"]
            datasetName = kwargs["dataset_name"]
            fixedMetadata = kwargs["fixed_metadata"]
            subscriptionName = kwargs["subscription_name"]
            filter = kwargs["filter"]
            replicationRules = kwargs["replication_rules"]
            comments = kwargs["comments"]
            subscriptionLifetime = kwargs["subscription_lifetime"]
            retroactive = kwargs["retroactive"]
            dryRun = kwargs["dry_run"]
            priority = kwargs["priority"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for test.")
            self.logger.critical(repr(e))
            return False

        rucio = RucioWrappersAPI()
        rucioAdmin = RucioAdminWrappersAPI()
        self.logger.info(rucio.ping())

        # Create a dataset to house the data, named with today's date
        # and scope <scope>.
        #
        self.logger.info("Creating a new dataset")
        datasetDID = createCollection(self.logger.name, scope, name=datasetName)

        # Set metadata on dataset
        #
        self.logger.info("Setting metadata on dataset {}".format(datasetDID))
        rucio.setMetadataBulk(datasetDID, fixedMetadata)

        # Generate random file of size <size> and upload.
        #
        f = generateRandomFile(size)
        fileDID = "{}:{}".format(scope, os.path.basename(f.name))
        try:
            self.logger.info("Uploading file ({}) to {}...".format(fileDID, uploadTo))
            rucio.upload(
                rse=uploadTo,
                scope=scope,
                filePath=f.name,
                lifetime=lifetime,
                logger=self.logger,
            )
            self.logger.debug("Upload complete")
        except Exception as e:
            self.logger.warning(repr(e))
        os.remove(f.name)

        # Attach to dataset
        self.logger.debug("Attaching file {} to {}".format(fileDID, datasetDID))
        try:
            rucio.attach(todid=datasetDID, dids=fileDID)
        except Exception as e:
            self.logger.warning(repr(e))
            return
        self.logger.debug("Attached file to dataset")

        # Set metadata and verify
        rucio.setMetadataBulk(datasetDID, fixedMetadata)
        retrieved_meta = rucio.getMetadata(datasetDID, plugin="DID_COLUMN")
        if not retrieved_meta == fixedMetadata:
            self.logger.warning("Retrieved metadata does not match input")

        # Create metadata-based subscription
        #
        self.logger.info("Creating/updating subscription {}".format(subscriptionName))
        existingSubs = rucioAdmin.listSubscriptions()
        if subscriptionName in (sub["name"] for sub in existingSubs):
            self.logger.info(
                "Subscription {} already exists, will update".format(subscriptionName)
            )
            rucioAdmin.updateSubscription(
                subscriptionName,
                account=os.environ["RUCIO_CFG_ACCOUNT"],
                filter=filter,
                replication_rules=replicationRules,
                comments=comments,
                lifetime=subscriptionLifetime,
                retroactive=retroactive,
                dry_run=dryRun,
                priority=priority,
            )
        else:
            self.logger.info("Creating subscription {}".format(subscriptionName))
            rucioAdmin.addSubscription(
                subscriptionName,
                account=os.environ["RUCIO_CFG_ACCOUNT"],
                filter=filter,
                replication_rules=replicationRules,
                comments=comments,
                lifetime=subscriptionLifetime,
                retroactive=retroactive,
                dry_run=dryRun,
                priority=priority,
            )

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
