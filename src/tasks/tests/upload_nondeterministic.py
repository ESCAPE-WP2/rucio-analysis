import datetime
import importlib
import os
import tempfile

from gfal2 import Gfal2Context

from common.rucio.wrappers import RucioWrappersAPI
from tasks.task import Task


class TestUploadNondeterministic(Task):
    """ Upload nondeterministic data to an RSE. """

    def __init__(self, logger):
        super().__init__(logger)

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            # Assign variables from kwargs.
            #
            rse = kwargs["rse"]
            lfnpfnSpooferClassName = kwargs["lfnpfn_spoofer_class_name"]
            lfnpfnSpooferKwargs = kwargs["lfnpfn_spoofer_kwargs"]
            scheme = kwargs["scheme"]
            hostname = kwargs["hostname"]
            prefix = kwargs["prefix"]
            scope = kwargs["scope"]
            filelistDir = kwargs["filelist_dir"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        # Set up GFAL2 context and transfer parameters.
        #
        gfal = Gfal2Context()
        params = gfal.transfer_parameters()
        params.set_checksum = True
        params.overwrite = True
        params.set_create_parent = True
        params.get_create_parent = True
        params.timeout = 300

        # Verify the scheme, hostname and prefix (protocol) is supported by this RSE.
        #
        rucio = RucioWrappersAPI()
        selectedProtocol = None
        for protocol in rucio.getRSEProtocols(rse):
            if protocol['scheme'] == scheme and \
                    protocol['hostname'] == hostname and \
                    protocol['prefix'] == prefix:
                selectedProtocol = '{}://{}{}'.format(scheme, hostname, prefix)
                break

        if not selectedProtocol:
            self.logger.critical("Protocol not supported by this RSE.")
            return False

        # Instantiate LFN/PFN spoofer.
        #
        try:
            spoofer = getattr(importlib.import_module(
                "common.rucio.lfnpfn_spoofer"), lfnpfnSpooferClassName)(
                    self.logger, scheme, hostname, prefix, scope)
        except ImportError as e:
            self.logger.critical("Could not import lfnpfn_spoofer module.")
            self.logger.critical(repr(e))
            return False
        except AttributeError as e:
            self.logger.critical("Requested class not found in lfnpfn_spoofer module.")
            self.logger.critical(repr(e))
            return False

        startTimestamp = datetime.datetime.now().strftime("%d%m%yT%H.%M.%S")
        spoofer.spoof(lfnpfnSpooferKwargs)

        # Open a file to keep list of added PFNs.
        # Format: <pfn>\t<did>\n per file.
        #
        with tempfile.NamedTemporaryFile(mode="w+") as filelist_p:
            filelist_p.write('# pfn did\n')

            # Ingest data.
            #
            for lfn, pfn in zip(spoofer.lfns, spoofer.pfns):
                self.logger.info("Uploading file with path {}".format(pfn.abspath))
                gfal.mkdir_rec(pfn.dirname, 775)
                gfal.filecopy(params, "file://" + lfn.abspath, pfn.abspath)

                filelist_p.write('{}\t{}:{}\n'.format(pfn.abspath, scope, pfn.name))

            # Flush buffer and write this file list.
            #
            filelist_p.flush()
            filelist_lfn = filelist_p.name
            filelist_pfn = os.path.join(
                selectedProtocol,
                filelistDir,
                'run_{}'.format(startTimestamp)
            )

            self.logger.info("Writing file list to {}".format(filelist_pfn))
            gfal.mkdir_rec(os.path.join(
                selectedProtocol,
                filelistDir), 775)
            gfal.filecopy(params, "file://" + filelist_lfn, filelist_pfn)

        self.toc()
        self.logger.info("Finished in {}s".format(round(self.elapsed)))
