# -*- coding: utf-8 -*-

from minerva.xmldochandler.qname import QName


def test_constructor():
    namespaceuri = "somename.org"
    localname = "directory"
    qname = QName(namespaceuri, localname)

    assert qname.namespacename == namespaceuri
    assert qname.localname == localname


def test_split():
    parts = QName.split("somename.org:directory")

    assert parts[0] == "somename.org"
    assert parts[1] == "directory"
