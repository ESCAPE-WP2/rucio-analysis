from abc import abstractmethod
import datetime
import os
import uuid

from common.rucio.lfn import LFN
from common.rucio.pfn import PFN
from utility import generateRandomFile


class LFNPFNSpoofer():
    def __init__(self, logger, scheme, hostname, prefix, scope):
        self.logger = logger
        self._scheme = scheme
        self._hostname = hostname
        self._prefix = prefix
        self._scope = scope
        self.mapping = []           # list of (LFN, PFN) tuples.

    def __del__(self):
        """ Remove all lfns. """
        for lfn in self.lfns:
            try:
                os.remove(lfn.abspath)
            except FileNotFoundError:
                pass

    @abstractmethod
    def spoof(self):
        """
        Method for generating a list of LFN/PFN tuples.
        """
        raise NotImplementedError

    @property
    def lfns(self):
        """ Get LFNs from mapping. """
        lfns, _ = zip(*self.mapping)
        return lfns

    @property
    def pfns(self):
        """ Get PFNs from mapping. """
        _, pfns = zip(*self.mapping)
        return pfns


class LFNPFNSpoofer_SKAO_Testing_v1(LFNPFNSpoofer):
    """
        Spoofs PFN with schema:
        scope/test_dir_prefix_<timestamp>/project<pi>/dataset<di>/<fi>_<uuid>
    """

    def __init__(self, logger, scheme, hostname, prefix, scope):
        super().__init__(logger, scheme, hostname, prefix, scope)

    def spoof(self, kwargs):
        try:
            # Assign variables from kwargs.
            #
            nDatasetsPerProject = kwargs["n_datasets_per_project"]
            nFilesPerDataset = kwargs["n_files_per_dataset"]
            nProjects = kwargs["n_projects"]
            size = kwargs['size']
            test_dir_prefix = kwargs["test_dir_prefix"]
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg.")
            self.logger.critical(repr(e))
            return False

        # Create PFN paths following naming schema.
        #
        pfns = []
        for pi in range(0, nProjects):
            for di in range(0, nDatasetsPerProject):
                for fi in range(0, nFilesPerDataset):
                    timestamp = datetime.datetime.now().strftime("%d%m%yT%H.%M.%S")
                    dir = os.path.join(
                        self._scope,
                        self._prefix,
                        test_dir_prefix + '_' + timestamp,
                        "project{}".format(pi),
                        "dataset{}".format(di)
                    )
                    name = "{}_{}".format(fi, str(uuid.uuid4()))
                    pfns.append(PFN(self._scheme, self._hostname, dir, name))

        # Generate local files to map to these PFN and return as a list of tuples.
        #
        lfns = [LFN(generateRandomFile(size, prefix=str(uuid.uuid4())).name)
                for pfn in pfns]
        self.mapping += [(lfn, pfn) for lfn, pfn in zip(lfns, pfns)]
