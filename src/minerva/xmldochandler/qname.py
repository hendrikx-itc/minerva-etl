# -*- coding: utf-8 -*-



class QName:
    """
    A qualified name that contains a namespace name or prefix and a local name.
    """

    def __init__(self, namespacename, localname):
        self.namespacename = namespacename
        self.localname = localname

    @staticmethod
    def split(fullname):
        """
        Split a full name containing a namespace and local name part and return
        them in a tuple.
        """
        colon_index = fullname.find(u':')

        if colon_index >= 0:
            return fullname[:colon_index], fullname[colon_index+1:]
        else:
            return None, fullname

    def __str__(self):
        if self.namespacename is not None:
            return "{0:s}:{1:s}".format(self.namespacename, self.localname)
        else:
            return str(self.localname)

    def __eq__(self, other):
        if not isinstance(other, QName):
            return False

        return (
            self.namespacename == other.namespacename and
            self.localname == other.localname
        )

    def __hash__(self):
        return self.__str__().__hash__()
