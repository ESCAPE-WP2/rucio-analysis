import datetime
import os
import uuid

from utility import generateRandomFile


class LFN2PFN():
    def __init__(self, scheme, hostname, rse_prefix, scope):
        self.scheme = scheme
        self.hostname = hostname
        self.rse_prefix = rse_prefix
        self.scope = scope
        self._pfns = []
        self._files = []

    def __del__(self):
        """ Remove all temporary files. """
        for fi in self._files:
            try:
                os.remove(fi.name)
            except FileNotFoundError:
                pass

    @property
    def files(self):
        """ Get files. """
        return self._files

    @property
    def pfns(self):
        """ Get PFNs. """
        return self._pfns

    def getDIDsFromPFNs(self):
        """ Get DIDs from PFNs. """
        dids = []
        for pfn in self._pfns:
            dids.append('{}:{}'.format(
                self.scope,
                os.path.basename(pfn)
            ))
        return dids

    def getDirectoriesFromPFNs(self):
        """ Get directory names from PFNs. """
        dirs = []
        for pfn in self._pfns:
            dirs.append(os.path.dirname(pfn))
        return dirs

    def getFilenamesFromPFNs(self):
        """ Get filenames from PFNs. """
        filenames = []
        for pfn in self._pfns:
            filenames.append(os.path.basename(pfn))
        return filenames

    def insert(self, fi, pfn):
        self.pfns.append(pfn)
        self.files.append(fi)


class LFN2PFN_SKAO_SpoofGeneric(LFN2PFN):
    def __init__(self, scheme, hostname, rse_prefix, scope, kwargs):
        super().__init__(scheme, hostname, rse_prefix, scope)
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
        timestamp = datetime.datetime.now().strftime("%d%m%yT%H.%M.%S")
        for pi in range(0, self.nProjects):
            for di in range(0, self.nDatasets):
                for fi in range(0, self.nFiles):
                    fp = generateRandomFile(self.size, prefix=str(uuid.uuid4()))
                    pfn = os.path.join(
                        self.scheme + '://' + self.hostname + self.rse_prefix,
                        self.scope,
                        self.test_dir_prefix + '_' + timestamp,
                        "project{}".format(pi),
                        "dataset{}".format(di),
                        os.path.basename(fp.name),  # name already contains timestamp
                    )
                    self.insert(fp, pfn)
