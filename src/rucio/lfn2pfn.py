import datetime
import os

from io import generateRandomFile


class LFN2PFN():
    def __init__(self, logger, scheme, hostname, rse_prefix, scope):
        self.logger = logger
        self.scheme = scheme
        self.hostname = hostname
        self.rse_prefix = rse_prefix
        self.scope = scope
        self.pfns = []
        self.files = []

    def __del__(self):
        """ Remove all temporary files. """
        for fi in self.files:
            os.remove(fi.name)

    @property
    def dids(self):
        """ Get DIDs from PFNs. """
        dids = []
        for pfn in self.pfns:
            dids.append('{}:{}'.format(
                self.scope,
                os.path.basename(pfn)
            ))
        return dids

    @property
    def directories(self):
        """ Get directory names from PFNs. """
        dirs = []
        for pfn in self.pfns:
            dirs.append(os.path.dirname(pfn))
        return dirs

    @property
    def filenames(self):
        """ Get filenames from PFNs. """
        filenames = []
        for pfn in self.pfns:
            filenames.append(os.path.basename(pfn))
        return filenames

    @property
    def pfns(self):
        """ Get PFNs. """
        return self.pfns

    def insert(self, fi, pfn):
        self.pfns.append(pfn)
        self.files.append(fi)


class LFN2PFN_SKAO_SpoofGeneric(LFN2PFN):
    def __init__(self, logger, scheme, hostname, rse_prefix, scope, kwargs):
        super().__init__(logger, scheme, hostname, rse_prefix, scope)
        try:
            # Assign variables from kwargs.
            #
            self.test_dir_prefix = kwargs["test_dir_prefix"]
            self.nFiles = kwargs["n_files"]
            self.nProjects = kwargs["n_projects"]
            self.nDatasets = kwargs["n_datasets"]
            self.size = kwargs['size']
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        # Paths for data uploaded are constructed as:
        # scope/test_dir_prefix_<timestamp>/project<pi>/dataset<di>/filename_<timestamp>
        #
        timestamp = datetime.now().strftime("%d%m%yT%H.%M.%S")
        for pi in range(0, self.nProjects):
            for di in range(0, self.nDatasets):
                for fi in range(0, self.nFiles):
                    f = generateRandomFile(self.size)
                    pfn = os.path.join(
                        self.scheme + '://' + self.hostname + self.rse_prefix,
                        self.scope,
                        self.test_dir_prefix_ + '_' + timestamp,
                        "project{}".format(pi),
                        "dataset{}".format(di),
                        os.path.basename(f.name) + '_' + timestamp,
                    )
                    self.insert(f, pfn)
