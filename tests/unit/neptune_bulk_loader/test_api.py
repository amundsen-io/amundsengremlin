# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import datetime
import unittest

import pytest
from flask import Flask

from amundsen_gremlin.config import LocalGremlinConfig
from amundsen_gremlin.gremlin_model import (
    EdgeTypes, MagicProperties, VertexTypes
)
from amundsen_gremlin.neptune_bulk_loader.api import (
    NeptuneBulkLoaderApi,
    get_neptune_graph_traversal_source_factory_from_config
)
from amundsen_gremlin.neptune_bulk_loader.gremlin_model_converter import (
    _GetGraph, new_entities, new_existing
)
from amundsen_gremlin.test_and_development_shard import (
    delete_graph_for_shard_only
)


@pytest.mark.roundtrip
class TestBulkLoader(unittest.TestCase):
    def setUp(self) -> None:
        self.maxDiff = None
        self.app = Flask(__name__)
        self.app_context = self.app.app_context()
        self.app.config.from_object(LocalGremlinConfig())
        self.app_context.push()
        self.bulk_loader = NeptuneBulkLoaderApi.create_from_config(self.app.config)
        self.neptune_graph_traversal_source_factory = get_neptune_graph_traversal_source_factory_from_config(self.app.config)
        self._drop_almost_everything()

    def _drop_almost_everything(self) -> None:
        delete_graph_for_shard_only(self.neptune_graph_traversal_source_factory())

    def tearDown(self) -> None:
        delete_graph_for_shard_only(self.neptune_graph_traversal_source_factory())
        self.app_context.pop()

    def test_failed_load_logs(self) -> None:
        created_at = datetime.datetime(2020, 5, 27, 10, 50, 50)
        entities = new_entities()
        existing = new_existing()
        _GetGraph._create(VertexTypes.Database, entities, existing, key='foo', name='foo')
        _GetGraph._create(EdgeTypes.Cluster, entities, existing, created=created_at, **{
            MagicProperties.FROM.value.name: VertexTypes.Database.value.id(key='foo'),
            MagicProperties.TO.value.name: VertexTypes.Database.value.id(key='bar'),
        })
        with self.assertLogs(logger='amundsen_gremlin.neptune_bulk_loader.api', level='WARNING') as logs:
            status = self.bulk_loader.bulk_load_entities(entities=entities)
        failed = [load_id for load_id, overall_status in status.items()
                  if overall_status['overallStatus']['status'] != 'LOAD_COMPLETED']
        self.assertEqual(2, len(status), f'expected 2 status = {status}')
        self.assertEqual(1, len(failed), f'expected 1 failed in status = {status}')
        self.assertEqual(1, len(logs.output), f'expected 1 output: {logs.output}')
        self.assertTrue(all(line.startswith('WARNING:amundsen_gremlin.neptune_bulk_loader.api:some loads failed:')
                            for line in logs.output),
                        f'expected output to start with some loads failed: {logs.output}')

    def test_failed_load_raises(self) -> None:
        created_at = datetime.datetime(2020, 5, 27, 10, 50, 50)
        entities = new_entities()
        existing = new_existing()
        _GetGraph._create(VertexTypes.Database, entities, existing, key='foo', name='foo')
        _GetGraph._create(EdgeTypes.Cluster, entities, existing, created=created_at, **{
            MagicProperties.FROM.value.name: VertexTypes.Database.value.id(key='foo'),
            MagicProperties.TO.value.name: VertexTypes.Database.value.id(key='bar'),
        })
        with self.assertLogs(logger='amundsen_gremlin.neptune_bulk_loader.api', level='WARNING') as logs:
            with self.assertRaisesRegex(AssertionError, 'some loads failed'):
                self.bulk_loader.bulk_load_entities(entities=entities, raise_if_failed=True)
        self.assertEqual(1, len(logs.output), f'expected 1 output: {logs.output}')
        self.assertTrue(all(line.startswith('WARNING:amundsen_gremlin.neptune_bulk_loader.api:some loads failed:')
                            for line in logs.output),
                        f'expected output to start with some loads failed: {logs.output}')
