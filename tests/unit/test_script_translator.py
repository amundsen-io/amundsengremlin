# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import datetime
import unittest

from gremlin_python.process.graph_traversal import __, addV, unfold
from gremlin_python.process.traversal import Cardinality

from amundsen_gremlin.script_translator import (
    ScriptTranslator, ScriptTranslatorTargetJanusgraph,
    ScriptTranslatorTargetNeptune
)


class ScriptTranslatorTest(unittest.TestCase):
    def test_upsert(self) -> None:
        g = __.V().has('User', 'key', 'jack').fold().coalesce(
            unfold(),
            addV('User').property(Cardinality.single, 'key', 'jack')). \
            coalesce(__.has('email', 'jack@example.com'),
                     __.property(Cardinality.single, 'email', 'jack@example.com')). \
            coalesce(__.has('url', 'https://twitter.com/jack'),
                     __.property(Cardinality.single, 'url', 'https://twitter.com/jack'))
        actual = ScriptTranslator.translateB('g', g)
        self.assertEqual(actual, '''g.V().has("User","key","jack").fold().coalesce(__.unfold(),__.addV("User").property(single,"key","jack")).coalesce(__.has("email","jack@example.com"),__.property(single,"email","jack@example.com")).coalesce(__.has("url","https://twitter.com/jack"),__.property(single,"url","https://twitter.com/jack"))''')  # noqa: E501

    def test_string_null(self) -> None:
        self.assertEqual(ScriptTranslator._convert_to_string(None), 'null')

    def test_string_bool(self) -> None:
        self.assertEqual(ScriptTranslator._convert_to_string(True), 'true')
        self.assertEqual(ScriptTranslator._convert_to_string(False), 'false')

    def test_string_char(self) -> None:
        # the printables minus the double quote
        for c in tuple(' !#$%&()*+,-.0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]^_`abcdefghijklmnopqrstuvwxyz{|}~'):
            actual = ScriptTranslator._convert_to_string(c)
            self.assertEqual(actual, f'"{c}"')

    def test_string_escaping_char(self) -> None:
        for c in '\'\\"':
            actual = ScriptTranslator._convert_to_string(c)
            self.assertEqual(actual, f'"\\{c}"')

    def test_string_escaping_control_or_unicode(self) -> None:
        for input, escaped in zip('\x00\x01\x02\x03\x04\x05\x06\x07\b\t\n\x0b\f\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16'
                                  + '\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f\x7f\x80\x81\x82\x83\x84\x85\x86\x87\x88\x89'
                                  + '\x8a\x8b\x8c\x8d\x8e\x8f\x90\x91\x92\x93\x94\x95',
                                  ('\\u0000', '\\u0001', '\\u0002', '\\u0003', '\\u0004', '\\u0005', '\\u0006',
                                   '\\u0007', '\\b', '\\t', '\\n', '\\u000b', '\\f', '\\r', '\\u000e', '\\u000f',
                                   '\\u0010', '\\u0011', '\\u0012', '\\u0013', '\\u0014', '\\u0015', '\\u0016',
                                   '\\u0017', '\\u0018', '\\u0019', '\\u001a', '\\u001b', '\\u001c', '\\u001d',
                                   '\\u001e', '\\u001f', '\\u007f', '\\u0080', '\\u0081', '\\u0082', '\\u0083',
                                   '\\u0084', '\\u0085', '\\u0086', '\\u0087', '\\u0088', '\\u0089', '\\u008a',
                                   '\\u008b', '\\u008c', '\\u008d', '\\u008e', '\\u008f', '\\u0090', '\\u0091',
                                   '\\u0092', '\\u0093', '\\u0094', '\\u0095')):
            actual = ScriptTranslator._convert_to_string(input)
            self.assertEqual(actual, f'"{escaped}"')

    def test_string_datetime_zero_millis_janusgraph(self) -> None:
        g = __.property(Cardinality.single, 'created', datetime.datetime(2010, 8, 31, 19, 55, 10))
        actual = ScriptTranslatorTargetJanusgraph.translateB('g', g)
        self.assertEqual(actual, '''g.property(single,"created",new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS").parse("2010-08-31T19:55:10.000000"))''')  # noqa: E501

    def test_string_datetime_some_millis_janusgraph(self) -> None:
        g = __.property(Cardinality.single, 'created', datetime.datetime(2010, 8, 31, 19, 55, 10, 123))
        actual = ScriptTranslatorTargetJanusgraph.translateB('g', g)
        self.assertEqual(actual, '''g.property(single,"created",new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSS").parse("2010-08-31T19:55:10.000123"))''')  # noqa: E501

    def test_string_date_janusgraph(self) -> None:
        g = __.property(Cardinality.single, 'created', datetime.date(2010, 8, 31))
        actual = ScriptTranslatorTargetJanusgraph.translateB('g', g)
        self.assertEqual(actual, '''g.property(single,"created",new java.text.SimpleDateFormat("yyyy-MM-dd").parse("2010-08-31"))''')  # noqa: E501

    def test_string_datetime_zero_millis_neptune(self) -> None:
        g = __.property(Cardinality.single, 'created', datetime.datetime(2010, 8, 31, 19, 55, 10))
        actual = ScriptTranslatorTargetNeptune.translateB('g', g)
        self.assertEqual(actual, '''g.property(single,"created",datetime("2010-08-31T19:55:10"))''')  # noqa: E501

    def test_string_datetime_some_millis_neptune(self) -> None:
        g = __.property(Cardinality.single, 'created', datetime.datetime(2010, 8, 31, 19, 55, 10, 123))
        actual = ScriptTranslatorTargetNeptune.translateB('g', g)
        self.assertEqual(actual, '''g.property(single,"created",datetime("2010-08-31T19:55:10.000123"))''')  # noqa: E501

    def test_string_date_neptune(self) -> None:
        g = __.property(Cardinality.single, 'created', datetime.date(2010, 8, 31))
        actual = ScriptTranslatorTargetNeptune.translateB('g', g)
        self.assertEqual(actual, '''g.property(single,"created",datetime("2010-08-31"))''')  # noqa: E501
