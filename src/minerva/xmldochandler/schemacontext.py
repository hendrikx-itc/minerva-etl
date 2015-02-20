# -*- coding: utf-8 -*-

__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2015 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from minerva.xmldochandler.xmlnamespace import XmlNamespace


class Substitution():

    def __init__(self, elementhandler, substitutiongroup):
        self.elementhandler = elementhandler
        self.substitutiongroup = substitutiongroup


class SchemaContext():

    def __init__(self):
        self.default_namespace = None
        self.namespaces = {}
        self.namespace_prefix_mappings = {}
        self.elementreferences = []
        self.basetypereferences = []
        self.substitutions = []
        self.all_types = []

    def get_elementhandler(self, namespace_uri, localname):
        """
        Return a root element handler from a name tuple
        (namespace_uri, localname)
        """
        namespace = self.namespaces.get(namespace_uri)

        if namespace:
            return namespace.root_elementhandlers.get(localname)
        elif self.default_namespace:
            return self.default_namespace.root_elementhandlers.get(localname)

        return None

    def get_elementtype(self, namespace_uri, name):
        namespace = self.namespaces.get(namespace_uri)

        elementtype = None

        if namespace:
            elementtype = namespace.named_types.get(name)

        return elementtype

    def get_elementhandler_by_path(self, namespace_uri, name_path):
        """
        Return an element handler from a name namespace_uri and a name
        path where the name path looks like the following example:
        /mdc/md/neid
        """
        namespace = self.namespaces.get(namespace_uri)

        if namespace:
            return namespace.get_elementhandler(name_path)

        return None

    def get_namespace(self, name):
        namespace = self.namespaces.get(name)

        if not namespace:
            namespace = XmlNamespace(name)
            self.namespaces[name] = namespace

        return namespace

    def link_basetypes(self):
        """
        Link base types to types
        """
        for basetypereference in self.basetypereferences:
            basetype = self.get_elementtype(
                basetypereference.ref.namespacename,
                basetypereference.ref.localname
            )

            if not basetype:
                raise Exception(
                    "Could not resolve base type [{0}]".format(
                        basetypereference.ref
                    )
                )

            basetypereference.setsubject(basetype)

    def link_substitutions(self):
        """
        Hack to support substitutions
        """
        for substitution in self.substitutions:
            for element_type in self.all_types:
                if substitution.substitutiongroup.localname in element_type.child_elementhandlers():
                    element_type.add_child_elementhandlers(
                        substitution.elementhandler
                    )

    def link_handlers(self):
        """
        Link the element handlers to the types
        """
        for (namespace_uri, namespace) in self.namespaces.iteritems():
            for elementrelation in namespace.elementrelations:
                elementhandler = self.get_elementhandler_by_path(
                    elementrelation.namespaceuri, elementrelation.elementpath
                )

                if not elementhandler:
                    raise Exception(
                        "Could not resolve relation to element handler with "
                        "path [{0}]".format(elementrelation.elementpath)
                    )

                elementrelation.elementtype.child_elementhandlers[elementhandler.name] = elementhandler

    def link_elementreferences(self):
        for elementreference in self.elementreferences:
            real_handler = self.get_elementhandler(
                elementreference.ref.namespacename,
                elementreference.ref.localname
            )

            if not real_handler:
                raise Exception(
                    "No handler found for reference {0}".format(
                        elementreference.ref
                    )
                )

            elementreference.setsubject(real_handler)
