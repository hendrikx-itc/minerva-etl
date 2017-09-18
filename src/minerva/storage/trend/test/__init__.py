# -*- coding: utf-8 -*-


class DataSet:
    description = "Set this property in child classes to describe the data set"

    def load(self, cursor):
        """
        Override this method in child class to load the test set.
        """
        raise NotImplementedError()
