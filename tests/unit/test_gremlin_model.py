# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import datetime
import unittest

import pytz
from gremlin_python.process.traversal import Cardinality

from amundsen_gremlin.gremlin_model import (
    EdgeType, EdgeTypes, GremlinCardinality, GremlinType, MagicProperties,
    Property, VertexType, VertexTypes, WellKnownProperties
)
from amundsen_gremlin.test_and_development_shard import get_shard


class TestGremlinEnums(unittest.TestCase):
    def test_enum_unique_labels(self) -> None:
        self.assertIsInstance(VertexTypes.by_label(), dict)
        self.assertIsInstance(EdgeTypes.by_label(), dict)

    def test_cardinality(self) -> None:
        self.assertEqual(Cardinality.set_, GremlinCardinality.set.gremlin_python_cardinality())


class TestGremlinTyper(unittest.TestCase):
    def test_boolean_type(self) -> None:
        self.assertEqual('True', GremlinType.Boolean.value.format(True))
        self.assertEqual('False', GremlinType.Boolean.value.format(False))
        with self.assertRaisesRegex(AssertionError, 'expected bool'):
            GremlinType.Boolean.value.is_allowed('hi')
        with self.assertRaisesRegex(AssertionError, 'expected bool'):
            GremlinType.Boolean.value.is_allowed('True')

    def test_byte_type(self) -> None:
        a_byte = 2**7 - 1
        self.assertEqual('127', GremlinType.Byte.value.format(a_byte))
        GremlinType.Byte.value.is_allowed(a_byte)
        with self.assertRaisesRegex(AssertionError, 'expected int in [[]-2[*][*]7, 2[*][*]7[)]'):
            GremlinType.Byte.value.is_allowed('hi')
        with self.assertRaisesRegex(AssertionError, 'expected int in [[]-2[*][*]7, 2[*][*]7[)]'):
            GremlinType.Byte.value.is_allowed(2**7)
        with self.assertRaisesRegex(AssertionError, 'expected int in [[]-2[*][*]7, 2[*][*]7[)]'):
            GremlinType.Byte.value.is_allowed(-(2**7+1))

    def test_short_type(self) -> None:
        a_short = 2**7 + 1
        self.assertEqual('129', GremlinType.Short.value.format(a_short))
        GremlinType.Short.value.is_allowed(a_short)
        with self.assertRaisesRegex(AssertionError, 'expected int in [[]-2[*][*]15, 2[*][*]15[)]'):
            GremlinType.Short.value.is_allowed('hi')
        with self.assertRaisesRegex(AssertionError, 'expected int in [[]-2[*][*]15, 2[*][*]15[)]'):
            GremlinType.Short.value.is_allowed(2**15)
        with self.assertRaisesRegex(AssertionError, 'expected int in [[]-2[*][*]15, 2[*][*]15[)]'):
            GremlinType.Short.value.is_allowed(-(2**15+1))

    def test_int_type(self) -> None:
        a_int = 2**15 + 1
        self.assertEqual('32769', GremlinType.Int.value.format(a_int))
        GremlinType.Int.value.is_allowed(a_int)
        with self.assertRaisesRegex(AssertionError, 'expected int in [[]-2[*][*]31, 2[*][*]31[)]'):
            GremlinType.Int.value.is_allowed('hi')
        with self.assertRaisesRegex(AssertionError, 'expected int in [[]-2[*][*]31, 2[*][*]31[)]'):
            GremlinType.Int.value.is_allowed(2**31)
        with self.assertRaisesRegex(AssertionError, 'expected int in [[]-2[*][*]31, 2[*][*]31[)]'):
            GremlinType.Int.value.is_allowed(-(2**31+1))

    def test_long_type(self) -> None:
        a_long = 2**31 + 1
        self.assertEqual('2147483649', GremlinType.Long.value.format(a_long))
        GremlinType.Long.value.is_allowed(a_long)
        with self.assertRaisesRegex(AssertionError, 'expected int in [[]-2[*][*]63, 2[*][*]63[)]'):
            GremlinType.Long.value.is_allowed('hi')
        with self.assertRaisesRegex(AssertionError, 'expected int in [[]-2[*][*]63, 2[*][*]63[)]'):
            GremlinType.Long.value.is_allowed(2**63)
        with self.assertRaisesRegex(AssertionError, 'expected int in [[]-2[*][*]63, 2[*][*]63[)]'):
            GremlinType.Long.value.is_allowed(-(2**63+1))

    def test_float_type(self) -> None:
        a_float = float(4/3)
        self.assertEqual('1.3333333333333333', GremlinType.Float.value.format(a_float))
        with self.assertRaisesRegex(AssertionError, 'expected float'):
            GremlinType.Float.value.format('hi')
        with self.assertRaisesRegex(AssertionError, 'expected float.'):
            GremlinType.Float.value.is_allowed('hi')
        GremlinType.Float.value.is_allowed(a_float)

    def test_string_type(self) -> None:
        a_str = 'hi'
        self.assertEqual('hi', GremlinType.String.value.format(a_str))
        with self.assertRaisesRegex(AssertionError, 'expected str'):
            GremlinType.String.value.format(10)
        with self.assertRaisesRegex(AssertionError, 'expected str'):
            GremlinType.String.value.is_allowed(10)
        GremlinType.String.value.is_allowed(a_str)

    def test_date_type(self) -> None:
        a_datetime = datetime.datetime(2020, 5, 27, 10, 50, 50, 924185)
        self.assertEqual('2020-05-27T10:50:50', GremlinType.Date.value.format(a_datetime))
        self.assertEqual('2020-05-27', GremlinType.Date.value.format(a_datetime.date()))
        with self.assertRaisesRegex(AssertionError, 'wat?'):
            GremlinType.Date.value.format('2020-05-27')
        with self.assertRaisesRegex(AssertionError, 'expected datetime.'):
            GremlinType.Date.value.is_allowed('2020-05-27')
        with self.assertRaisesRegex(AssertionError, 'expected datetime.'):
            GremlinType.Date.value.is_allowed(a_datetime.astimezone(pytz.utc))
        GremlinType.Date.value.is_allowed(a_datetime)
        GremlinType.Date.value.is_allowed(a_datetime.date())


class TestVertexType(unittest.TestCase):
    def test_as_map(self) -> None:
        self.assertIsInstance(VertexTypes.Column.value.properties_as_map(), dict)

    def test_create(self) -> None:
        actual = VertexTypes.Column.value.create(key='column_key', name='name', col_type=None)
        self.assertSetEqual(set(actual.keys()),
                            {MagicProperties.LABEL.value.name, MagicProperties.ID.value.name,
                             WellKnownProperties.TestShard.value.name, 'key', 'name'})
        self.assertEqual(actual.get(MagicProperties.LABEL.value.name), 'Column')
        self.assertEqual(actual.get(MagicProperties.ID.value.name), f'{get_shard()}:Column:column_key')
        self.assertEqual(actual.get('key'), 'column_key')

    def test_create_type_explodes_if_id_format(self) -> None:
        with self.assertRaisesRegex(AssertionError, 'id_format: {shard}:{foo}:bar has parameters:'):
            VertexType.construct_type(id_format='{foo}:bar')


class TestEdgeType(unittest.TestCase):
    def test_as_map(self) -> None:
        self.assertIsInstance(EdgeTypes.Column.value.properties_as_map(), dict)

    def test_create(self) -> None:
        created_at = datetime.datetime(2020, 5, 27, 10, 50, 50, 924185)
        actual = EdgeTypes.Column.value.create(created=created_at, expired=None, **{
            MagicProperties.FROM.value.name: VertexTypes.Column.value.id(key='column_key'),
            MagicProperties.TO.value.name: VertexTypes.Table.value.id(key='table_key'),
        })
        self.assertSetEqual(
            set(actual.keys()),
            set([e.value.name for e in (MagicProperties.LABEL, MagicProperties.ID, MagicProperties.FROM,
                                        MagicProperties.TO, WellKnownProperties.Created)]))
        self.assertEqual(actual.get(MagicProperties.LABEL.value.name), 'COLUMN')
        self.assertEqual(actual.get(MagicProperties.ID.value.name),
                         f'COLUMN:2020-05-27T10:50:50:{get_shard()}:Column:column_key->{get_shard()}:Table:table_key')

    def test_create_type_explodes_if_id_format(self) -> None:
        with self.assertRaisesRegex(AssertionError, 'id_format: {foo}:bar has parameters:'):
            EdgeType.construct_type(id_format='{foo}:bar')


class TestProperty(unittest.TestCase):
    def test_signature(self) -> None:
        expected = Property(name='foo', type=GremlinType.String, cardinality=GremlinCardinality.list)
        actual = Property(name='foo', type=GremlinType.String, comment='a bar').signature(GremlinCardinality.list)
        self.assertEqual(expected, actual)

    def test_format(self) -> None:
        a_datetime = datetime.datetime(2020, 5, 27, 10, 50, 50, 924185)
        a_property = Property(name='date', type=GremlinType.Date)
        self.assertEqual('2020-05-27T10:50:50', a_property.format(a_datetime))

    def test_header(self) -> None:
        self.assertEqual('foo:Date', Property(name='foo', type=GremlinType.Date).header())
        self.assertEqual('foo:Date(single)',
                         Property(name='foo', type=GremlinType.Date, cardinality=GremlinCardinality.single).header())
        self.assertEqual('foo:Date(set)',
                         Property(name='foo', type=GremlinType.Date, cardinality=GremlinCardinality.set).header())
        self.assertEqual('foo:Date(list)',
                         Property(name='foo', type=GremlinType.Date, cardinality=GremlinCardinality.list).header())
        self.assertEqual('foo:Date(single)[]', Property(
            name='foo', type=GremlinType.Date, cardinality=GremlinCardinality.single, multi_valued=True).header())
        self.assertEqual('foo:Date(set)[]', Property(
            name='foo', type=GremlinType.Date, cardinality=GremlinCardinality.set, multi_valued=True).header())
        self.assertEqual('foo:Date(list)[]', Property(
            name='foo', type=GremlinType.Date, cardinality=GremlinCardinality.list, multi_valued=True).header())

    def test_magic_header(self) -> None:
        self.assertEqual('~label', MagicProperties.LABEL.value.header())
        self.assertEqual('~id', MagicProperties.ID.value.header())
        self.assertEqual('~from', MagicProperties.FROM.value.header())
        self.assertEqual('~to', MagicProperties.TO.value.header())
