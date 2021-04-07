# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import os
import unittest
from unittest import mock

from amundsen_common.tests.fixtures import Fixtures

from amundsen_gremlin.test_and_development_shard import (
    _reset_for_testing_only, _shard_default, get_shard, shard_set_explicitly
)


class TestTestShard(unittest.TestCase):
    def setUp(self) -> None:
        _reset_for_testing_only()

    def tearDown(self) -> None:
        _reset_for_testing_only()

    def test_set_shard_works(self) -> None:
        expected = Fixtures.next_string()
        shard_set_explicitly(expected)
        actual = get_shard()
        self.assertEqual(expected, actual)

    def test_set_shard_explodes(self) -> None:
        expected = get_shard()
        with self.assertRaisesRegex(AssertionError, 'can only shard_set_explicitly if it has not been used yet.'):
            shard_set_explicitly('x')
        actual = get_shard()
        self.assertEqual(expected, actual)

    def test_shard_default_ci(self) -> None:
        with mock.patch.dict(os.environ):
            os.environ['CI'] = 'x'
            os.environ['BUILD_PART_ID'] = '12345'
            os.environ.pop('DATACENTER', None)
            os.environ['USER'] = 'jack'
            actual = _shard_default()
            self.assertEqual('12345', actual)

    def test_shard_default_local(self) -> None:
        with mock.patch.dict(os.environ):
            os.environ.pop('CI', None)
            os.environ.pop('BUILD_PART_ID', None)
            os.environ.pop('DATACENTER', None)
            os.environ['USER'] = 'jack'
            actual = _shard_default()
            self.assertEqual('jack', actual)

    def test_shard_default_also_local(self) -> None:
        with mock.patch.dict(os.environ):
            os.environ.pop('CI', None)
            os.environ.pop('BUILD_PART_ID', None)
            os.environ.pop('DATACENTER', 'local')
            os.environ['USER'] = 'jack'
            actual = _shard_default()
            self.assertEqual('jack', actual)

    def test_shard_default_environment_production(self) -> None:
        with mock.patch.dict(os.environ):
            os.environ.pop('CI', None)
            os.environ.pop('BUILD_PART_ID', None)
            os.environ['DATACENTER'] = 'x'
            os.environ['USER'] = 'jack'
            os.environ['ENVIRONMENT'] = 'production'
            actual = _shard_default()
            self.assertIsNone(actual)

    def test_shard_default_environment_ignoring_shard(self) -> None:
        with mock.patch.dict(os.environ):
            os.environ.pop('CI', None)
            os.environ.pop('BUILD_PART_ID', None)
            os.environ['IGNORE_NEPTUNE_SHARD'] = 'True'
            actual = _shard_default()
            self.assertIsNone(actual)

    def test_shard_default_environment_staging(self) -> None:
        with mock.patch.dict(os.environ):
            os.environ.pop('CI', None)
            os.environ.pop('BUILD_PART_ID', None)
            os.environ['DATACENTER'] = 'x'
            os.environ['USER'] = 'jack'
            os.environ['ENVIRONMENT'] = 'staging'
            actual = _shard_default()
            self.assertIsNone(actual)
