import os


class PFN():
    def __init__(self, scheme, hostname, dir, name):
        self._scheme = scheme.strip('/')
        self._hostname = hostname.strip('/')
        if self._scheme == "root":
            self._dir = '/' + dir.lstrip('/').rstrip('/')
        else:
            self._dir = dir.lstrip('/').rstrip('/')
        self._name = name.lstrip('/')

    @classmethod
    def fromabspath(cls, abspath):
        """ Deconstruct to PFN from abspath. """
        scheme = abspath.split('://')[0]
        hostname = abspath.split('://')[1].split('/')[0]
        if scheme == "root":
            dir = '/'.join(abspath.split('://')[1].split('/')[1:-1])
        else:
            dir = '/' + '/'.join(abspath.split('://')[1].split('/')[1:-1])
        name = abspath.split('://')[1].split('/')[-1]
        return cls(scheme, hostname, dir, name)

    @property
    def abspath(self):
        return '{}://{}/{}'.format(
            self._scheme,
            self._hostname,
            os.path.join(
                self._dir,
                self._name,
            )
        )

    @property
    def dir(self):
        return self._dir

    @property
    def dirname(self):
        return os.path.dirname(self.abspath)

    @property
    def name(self):
        return self._name
