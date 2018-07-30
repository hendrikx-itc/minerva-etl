# -*- coding: utf-8 -*-
import unittest

from minerva.directory.distinguishedname import \
        explode, implode, split_parts, escape, entity_type_name_from_dn, DistinguishedName


class TestDistinguishedName(unittest.TestCase):
    def test_explode(self):
        """
        Check that distinguished names are exploded correctly
        """
        exploded = explode(
            "Well,number=10,and,othernumber=20,notanumber=,thirdnumber= "
            )
        self.assertEqual(len(exploded), 3)
        self.assertEqual(exploded[0], ("number", "10"))
        self.assertEqual(exploded[1], ("othernumber", "20"))
        self.assertEqual(exploded[2], ("thirdnumber", " "))

    def test_implode(self):
        """
        Check that distinguished names are imploded correctly
        """
        imploded = implode([("SubNetwork", "NL1_R"),
                            ("Number", "17"),
                            ("Empty", ""),
                            ("UeRc", "9")])
        self.assertEqual(imploded, "SubNetwork=NL1_R,Number=17,Empty=,UeRc=9")
        
    def test_splitparts(self):
        """
        Check that distinguished names are split correctly
        """
        dn_parts = split_parts(
            "SubNetwork=NL1_R,SubNetwork=AHPTUR1,"
            "MeContext=AHPTUR1,ManagedElement=1,RncFunction=1,UeRc=9"
        )
        self.assertEqual(dn_parts[0], "SubNetwork=NL1_R")
        self.assertEqual(len(dn_parts), 6)
        self.assertEqual(dn_parts[5], "UeRc=9")
    
        dn_parts = split_parts(
            "Word=asdf,Writer=qwerty\\,dvorak,Reader=Unicode"
        )
        self.assertEqual(dn_parts[0], "Word=asdf")
        self.assertEqual(len(dn_parts), 3)
        self.assertEqual(dn_parts[2], "Reader=Unicode")
    
    def test_escape(self):
        """
        Check that ',' is escaped correctly
        """
        self.assertEqual(escape("Word=asdf,fdsa$2"), "Word=asdf\\,fdsa$2")
    
    def test_constructor(self):
        empty_dn = DistinguishedName([])
    
        self.assertEqual(len(empty_dn.parts), 0)
    
    def test_from_str(self):
        dn = DistinguishedName.from_str('Network=Global,Node=001')
    
        self.assertEqual(len(dn.parts), 2)
    
    def test_entitytype_name(self):
        dn = DistinguishedName.from_str('Network=Global,Node=001')
    
        self.assertEqual(dn.entity_type_name(), 'Node')

    def test_entity_type_name_from_dn(self):
        self.assertEqual(entity_type_name_from_dn('Network=Global,Node=001'), 'Node')

