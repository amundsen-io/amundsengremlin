# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import unittest

from gremlin_python.process.graph_traversal import __

from amundsen_gremlin.gremlin_shared import (
    append_traversal, get_database_name_from_uri, make_cluster_uri,
    make_column_uri, make_database_uri, make_description_uri, make_schema_uri,
    make_table_uri, rsubstringstartingwith
)


class TestGremlinShared(unittest.TestCase):
    def test_make_database_uri_et_al(self) -> None:
        self.assertEqual('database://BigQuery', make_database_uri(database_name='BigQuery'))
        self.assertEqual('BigQuery://neverland',
                         make_cluster_uri(cluster_name='neverland', database_uri='database://BigQuery'))
        self.assertEqual('BigQuery://neverland',
                         make_cluster_uri(cluster_name='neverland', database_name='BigQuery'))
        self.assertEqual('BigQuery://neverland.production',
                         make_schema_uri(schema_name='production', cluster_uri='BigQuery://neverland'))
        self.assertEqual('BigQuery://neverland.production',
                         make_schema_uri(schema_name='production', cluster_name='neverland', database_name='BigQuery'))
        self.assertEqual('BigQuery://neverland.production/lost_boys',
                         make_table_uri(table_name='lost_boys', schema_uri='BigQuery://neverland.production'))
        self.assertEqual('BigQuery://neverland.production/lost_boys',
                         make_table_uri(table_name='lost_boys', schema_name='production', cluster_name='neverland',
                                        database_name='BigQuery'))
        self.assertEqual('BigQuery://neverland.production/lost_boys/peter_pan',
                         make_column_uri(column_name='peter_pan',
                                         table_uri='BigQuery://neverland.production/lost_boys'))
        self.assertEqual('BigQuery://neverland.production/lost_boys/_user_description',
                         make_description_uri(source='user',
                                              subject_uri='BigQuery://neverland.production/lost_boys'))
        self.assertEqual('BigQuery://neverland.production/lost_boys/peter_pan/_user_description',
                         make_description_uri(
                             source='user',
                             subject_uri='BigQuery://neverland.production/lost_boys/peter_pan'))

    def test_get_database_name_from_uri_exceptional(self) -> None:
        self.assertEqual(None, rsubstringstartingwith('://', 'foo'))
        self.assertEqual('foo', rsubstringstartingwith('database://', 'database://foo'))
        with self.assertRaisesRegex(RuntimeError, 'database_uri is malformed! foo'):
            get_database_name_from_uri(database_uri='foo')

    def test_append_traversal(self) -> None:
        g = __.V().hasLabel('Foo')
        w = __.where(__.inE().outV().hasLabel('Bar'))
        actual = append_traversal(g, w)
        expected = __.V().hasLabel('Foo').where(__.inE().outV().hasLabel('Bar'))
        self.assertEqual(actual, expected)
