# -*- coding: utf-8 -*-

__docformat__ = "restructuredtext en"

__copyright__ = """
Copyright (C) 2008-2010 Hendrikx-ITC B.V.

Distributed under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 3, or (at your option) any later
version.  The full license is in the file COPYING, distributed as part of
this software.
"""
from urllib.parse import urlsplit


class SchemaElement():

    def __init__(self):
        # Reference to the parent element in the schema.
        self.parent = None
        self._children = []

    def add_child(self, element):
        """
        Add the element to the list of children and return it
        """
        self._children.append(element)

        return element

    def get_children(self):
        return self._children


class FractionDigits(SchemaElement):

    def __init__(self, value=1):
        SchemaElement.__init__(self)
        self.value = value

    def __str__(self):
        return "digits({0!s})".format(self.value)


class MinInclusive(SchemaElement):

    def __init__(self, value=0):
        SchemaElement.__init__(self)
        self.value = value

    def __str__(self):
        return "min({0!s})".format(self.value)


class MaxInclusive(SchemaElement):

    def __init__(self, value=0):
        SchemaElement.__init__(self)
        self.value = value

    def __str__(self):
        return "max({0!s})".format(self.value)


class MaxLength(SchemaElement):

    def __init__(self, length=100):
        SchemaElement.__init__(self)
        self.value = length

    def __str__(self):
        return "maxLength({0!s})".format(self.value)


class Enumeration(SchemaElement):

    def __init__(self, value):
        SchemaElement.__init__(self)
        self.value = value

    def __str__(self):
        return "enumeration({0!s})".format(self.value)


class Restriction(SchemaElement):

    def __init__(self):
        SchemaElement.__init__(self)
        # Can be a built in type like e.g. 'integer', 'decimal' or 'string'
        self.base = None
        self.mininclusive = None
        self.maxinclusive = None

    def __str__(self):
        return "restriction"


class SimpleType(SchemaElement):

    def __init__(self, name=None):
        SchemaElement.__init__(self)
        self.name = name
        self.restriction = None
        self.union = None

    def __str__(self):
        basetype = "Unknown"
        name = "Unnamed"

        if self.restriction is not None:
            basetype = str(self.restriction.base)

        if self.name is not None:
            name = self.name

        return "{0}({1})".format(name, basetype)


class Union(SchemaElement):

    def __init__(self):
        SchemaElement.__init__(self)

    def __str__(self):
        return "union"


class Sequence(SchemaElement):

    def __init__(self):
        SchemaElement.__init__(self)

    def __str__(self):
        return "sequence"


class All(SchemaElement):

    def __init__(self):
        SchemaElement.__init__(self)

    def __str__(self):
        return "all"


class Choice(SchemaElement):

    def __init__(self):
        SchemaElement.__init__(self)

    def __str__(self):
        return "choice"


class List(SchemaElement):

    def __init__(self, itemtype=None):
        SchemaElement.__init__(self)
        self.itemtype = itemtype

    def __str__(self):
        return "list"


class ComplexContent(SchemaElement):

    def __init__(self):
        SchemaElement.__init__(self)
        self.extension = None

    def add_child(self, element):
        if isinstance(element, Extension):
            self.extension = element

        return SchemaElement.add_child(self, element)

    def __str__(self):
        return "complexcontent"


class TypeReference():
    """ A container to hold a reference to a type. This can first be just
    the qname and later also a reference to the real type."""

    def __init__(self, name):
        self.name = name
        self.type = None


class ComplexType(SchemaElement):

    def __init__(self):
        SchemaElement.__init__(self)
        self.name = None
        self.abstract = False
        self.complexcontent = None
        self.simplecontent = None
        self.basetyperef = None
        self.elements = []
        self._attributes = []

    def add_child(self, element):
        if isinstance(element, ComplexContent):
            self.complexcontent = element

        return SchemaElement.add_child(self, element)

    def __str__(self):
        if self.name is not None:
            return "complextype {0:s}".format(self.name)
        else:
            return "complextype"

    def getAttributes(self):
        return self._attributes

    def addAttribute(self, attribute):
        self._attributes.append(attribute)

    attributes = property(fget=getAttributes)


class SimpleContent(SchemaElement):

    def __init__(self):
        SchemaElement.__init__(self)
        self.extension = None

    def __str__(self):
        return "simplecontent"


class Extension(SchemaElement):

    def __init__(self):
        SchemaElement.__init__(self)
        self.sequence = None
        self.basetypereference = None

    def addchild(self, element):
        if isinstance(element, Sequence):
            self.sequence = element

        return SchemaElement.add_child(self, element)

    def __str__(self):
        return "extension(base={0!s})".format(self.basetypereference)


class Attribute(SchemaElement):

    def __init__(self):
        SchemaElement.__init__(self)

        self._name = None
        self.type = None
        self.use = None
        self.ref = None

    def __str__(self):
        return "attribute({0:s})".format(self.name)

    def get_name(self):
        return self._name

    def set_name(self, name):
        if self.ref is not None:
            raise Exception("Error: ref and name cannot both be present!")
        self._name = name

    name = property(fget=get_name, fset=set_name)


class SubstitutionGroup():

    def __init__(self, name):
        self.name = name
        self.element = None


class Element(SchemaElement):

    def __init__(self, name=None):
        SchemaElement.__init__(self)

        # The parent element as it will appear in an XML document
        self.xml_element_parent = None
        self._name = name
        # An ElementReference object
        self.ref = None
        self.typename = None
        self.namespace = None
        self.substitutiongroup = None
        self.minoccurs = None
        self.maxoccurs = None
        self.complextype = None

    def add_child(self, element):
        if isinstance(element, ComplexType):
            self.complextype = element

        return SchemaElement.add_child(self, element)

    def build_elementstack(self):
        """
        Builds a stack representing the path from the root element to this
        element.
        """
        stack = []
        current_element = self

        while current_element:
            stack.insert(0, current_element)

            current_element = current_element.parent

            while current_element and not (isinstance(current_element, Element)):
                current_element = current_element.parent

        return stack

    def build_fullname(self):
        """Builds a full name for an element like:
        /ManagementNode/attributes/userLabel
        This is supposed to be more or less like an XPath query
        """
        elementstack = self.build_elementstack()

        names = []

        for element in elementstack:
            if element.ref:
                names.append(element.ref.localname)
            else:
                names.append(element.name)

        return "/" + "/".join(names)

    def __str__(self):
        if self.name is not None:
            return self.name
        else:
            return "ref({0:s})".format(self.ref)

    def get_name(self):
        if self._name:
            return self._name
        else:
            return self.ref.localname

    def set_name(self, name):
        self._name = name

    name = property(fget=get_name, fset=set_name)


class Import(SchemaElement):

    def __init__(self):
        SchemaElement.__init__(self)

        self.namespaceUri = None
        self.schemalocation = None

    def __str__(self):
        return "import({0:s})".format(self.namespaceUri)


class Include(SchemaElement):

    def __init__(self):
        SchemaElement.__init__(self)

        self.targetnamespace = None
        self.schemalocation = None

    def __str__(self):
        return "include"


class Schema(SchemaElement):

    def __init__(self):
        SchemaElement.__init__(self)

        self.filename = None
        self.targetnamespace = None
        self.defaultnamespace = None
        self.namespaces = {}
        self.prefixmappings = {}
        self.name = None
        self.includes = []

    def __str__(self):
        return "schema({0:s})".format(self.filename)

    def set_targetnamespace(self, uri):
        if self.namespaces.has_key(uri):
            self.targetnamespace = self.namespaces[uri]
        else:
            self.targetnamespace = Namespace(uri)
            self.namespaces[uri] = self.targetnamespace

        urlelements = urlsplit(uri)
        self.name = urlelements[4]

    def resolve_type(self, typename):
        """
        Resolves an element type by its name
        """
        type_ = None

        if typename is not None:
            if typename.namespacename is not None:
                namespace = self.prefixmappings.get(typename.namespacename)
            else:
                namespace = self.defaultnamespace

            if namespace is not None:
                type_ = namespace.types.get(typename.localname)
            else:
                raise Exception('No matching namespace found!')

        return type_

    def get_substitutiongroup(self, qname):
        if qname.namespacename is not None:
            namespace = self.prefixmappings.get(qname.namespacename)
        else:
            namespace = self.defaultnamespace

        if namespace is not None:
            substitutiongroup = namespace.get_substitutiongroup(qname.localname)
        else:
            substitutiongroup = SubstitutionGroup(qname.localname)

        return substitutiongroup

    def get_typereference(self, qname):
        if qname.namespacename is not None:
            namespace = self.prefixmappings.get(qname.namespacename)
        else:
            namespace = self.defaultnamespace

        if namespace is not None:
            typereference = namespace.get_typereference(qname.localname)
        else:
            typereference = TypeReference(qname.localname)

        return typereference

    def resolve_element(self, elementname):
        """
        Resolves an element by its name
        """
        element = None

        if elementname is not None:
            if elementname.namespacename is not None:
                namespace = self.prefixmappings.get(elementname.namespacename)
            else:
                namespace = self.defaultnamespace

            if namespace is not None:
                element = namespace.elements.get(elementname.localname)
            else:
                raise Exception("No matching namespace found")

        return element
