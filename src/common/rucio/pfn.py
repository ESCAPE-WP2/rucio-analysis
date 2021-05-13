import os


class PFN():
    def __init__(self, scheme, hostname, prefix, scope, name):
        self._scheme = scheme
        self._hostname = hostname
        self._prefix = prefix
        self._scope = scope
        self._name = name

    @property
    def did(self):
        return "{}:{}".format(self._scope, self._name)

    @property
    def dir(self):
        return os.path.dirname(self.path)

    @property
    def name(self):
        return self._name

    @property
    def protocol(self):
        return '{}://{}{}'.format(self._scheme, self._hostname, self._prefix)

    @property
    def scope(self):
        return self._scope

    @property
    def path(self):
        return os.path.join(self.protocol, self._scope, self._name)
