# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import datetime
import unittest
from operator import attrgetter
from typing import (
    Any, Callable, Dict, Hashable, Iterable, Mapping, Optional, Tuple, TypeVar,
    Union
)
from unittest import mock

import pytest
from amundsen_common.models.table import (
    Application, Column, ProgrammaticDescription, Table, Tag
)
from amundsen_common.models.user import User
from amundsen_common.tests.fixtures import Fixtures
from flask import Flask

from amundsen_gremlin.config import LocalGremlinConfig
from amundsen_gremlin.gremlin_model import (
    EdgeType, EdgeTypes, MagicProperties, VertexType, VertexTypes
)
from amundsen_gremlin.neptune_bulk_loader.api import (
    GraphEntity, GraphEntityType, NeptuneBulkLoaderApi,
    get_neptune_graph_traversal_source_factory_from_config
)
from amundsen_gremlin.neptune_bulk_loader.gremlin_model_converter import (
    ENTITIES, EXISTING, GetGraph, _FetchExisting,
    possible_application_names_application_key
)
from amundsen_gremlin.test_and_development_shard import (
    delete_graph_for_shard_only, get_shard
)

# TODO: add fetch test existing tests


def _create_one_expected(_type: Union[VertexType, EdgeType], **properties: Any) -> Tuple[str, Mapping[str, Any]]:
    if isinstance(_type, EdgeType):
        for property in (MagicProperties.FROM.value, MagicProperties.TO.value):
            value = properties.get(property.name)
            # as a convenience construct the id from the properties (since it will usually have all kinds of test
            # shards)
            if isinstance(value, Mapping) and MagicProperties.LABEL.value.name in value:
                id = VertexTypes.by_label()[value[MagicProperties.LABEL.value.name]].value.id(**value)
                properties[property.name] = id
            property.type.value.is_allowed(properties.get(property.name))

    entity = _type.create(**properties)
    return entity[MagicProperties.ID.value.name], entity


def _create_expected(expected: Mapping[Union[VertexType, EdgeType], Iterable[Mapping[str, Any]]]) -> ENTITIES:
    return {_type: dict(_create_one_expected(_type, **properties) for properties in entities)  # type: ignore
            for _type, entities in expected.items()}


@mock.patch('amundsen_gremlin.neptune_bulk_loader.gremlin_model_converter._FetchExisting')
class TestGetGraph(unittest.TestCase):
    def setUp(self) -> None:
        self.maxDiff = None

    def test_table_entities(self, fetch_existing: Any) -> None:
        table1 = Table(
            database='Snowflake', cluster='production', schema='esikmo', name='igloo',
            description='''it's cool''',
            programmatic_descriptions=[ProgrammaticDescription(text='super cool', source='other')],
            table_writer=Application(id='eskimo'),
            columns=[
                Column(name='block1', col_type='ice', sort_order=1, description='won'),
                Column(name='block2', col_type='ice', sort_order=2),
            ],
            tags=[Tag(tag_name='Kewl', tag_type='default')],
        )
        table2 = Table(database=table1.database, cluster=table1.cluster, schema=table1.schema, name='floes', columns=[],
                       table_writer=table1.table_writer)
        table_data = [table1, table2]

        def side_effect(*args: Any, existing: EXISTING, **kwargs: Any) -> None:
            _FetchExisting._fake_into_existing_vertexes_for_testing(
                _existing=existing, _type=VertexTypes.Application, id='eskimo', key='eskimo')

        fetch_existing.table_entities.side_effect = side_effect

        created_at = datetime.datetime(2020, 5, 27, 10, 50, 50, 924185)
        expected = _create_expected({
            VertexTypes.Database.value: [
                {'key': 'database://Snowflake', 'name': 'Snowflake'}],
            EdgeTypes.Cluster.value: [
                {'created': created_at, '~from': {'~label': 'Database', 'key': 'database://Snowflake'},
                 '~to': {'~label': 'Cluster', 'key': 'Snowflake://production'}}],
            VertexTypes.Cluster.value: [
                {'key': 'Snowflake://production', 'name': 'production'}],
            EdgeTypes.Schema.value: [
                {'created': created_at, '~from': {'~label': 'Cluster', 'key': 'Snowflake://production'},
                 '~to': {'~label': 'Schema', 'key': 'Snowflake://production.esikmo'}}],
            VertexTypes.Schema.value: [
                {'key': 'Snowflake://production.esikmo', 'name': 'esikmo'}],
            EdgeTypes.Table.value: [
                {'created': created_at, '~from': {'~label': 'Schema', 'key': 'Snowflake://production.esikmo'},
                 '~to': {'~label': 'Table', 'key': 'Snowflake://production.esikmo/igloo'}},
                {'created': created_at, '~from': {'~label': 'Schema', 'key': 'Snowflake://production.esikmo'},
                 '~to': {'~label': 'Table', 'key': 'Snowflake://production.esikmo/floes'}}],
            VertexTypes.Table.value: [
                {'is_view': False, 'key': 'Snowflake://production.esikmo/igloo', 'name': 'igloo'},
                {'is_view': False, 'key': 'Snowflake://production.esikmo/floes', 'name': 'floes'}],
            EdgeTypes.Tag.value: [
                {'created': created_at, '~from': {'~label': 'Tag', 'key': 'Kewl'},
                 '~to': {'~label': 'Table', 'key': 'Snowflake://production.esikmo/igloo'}}],
            VertexTypes.Tag.value: [
                {'key': 'Kewl', 'tag_name': 'Kewl', 'tag_type': 'default'}],
            EdgeTypes.LastUpdatedAt.value: [
                {'created': created_at, '~from': {'~label': 'Table', 'key': 'Snowflake://production.esikmo/igloo'},
                 '~to': {'~label': 'Updatedtimestamp', 'key': 'Snowflake://production.esikmo/igloo'}},
                {'created': created_at, '~from': {'~label': 'Table', 'key': 'Snowflake://production.esikmo/floes'},
                 '~to': {'~label': 'Updatedtimestamp', 'key': 'Snowflake://production.esikmo/floes'}}],
            VertexTypes.Updatedtimestamp.value: [
                {'key': 'amundsen_updated_timestamp', 'latest_timestamp': created_at},
                {'key': 'Snowflake://production.esikmo/igloo', 'latest_timestamp': created_at},
                {'key': 'Snowflake://production.esikmo/floes', 'latest_timestamp': created_at}],
            EdgeTypes.Column.value: [
                {'created': created_at, '~from': {'~label': 'Table', 'key': 'Snowflake://production.esikmo/igloo'},
                 '~to': {'~label': 'Column', 'key': 'Snowflake://production.esikmo/igloo/block1'}},
                {'created': created_at, '~from': {'~label': 'Table', 'key': 'Snowflake://production.esikmo/igloo'},
                 '~to': {'~label': 'Column', 'key': 'Snowflake://production.esikmo/igloo/block2'}}],
            VertexTypes.Column.value: [
                {'col_type': 'ice', 'key': 'Snowflake://production.esikmo/igloo/block1',
                 'name': 'block1', 'sort_order': 1},
                {'col_type': 'ice', 'key': 'Snowflake://production.esikmo/igloo/block2',
                 'name': 'block2', 'sort_order': 2}],
            EdgeTypes.Description.value: [
                {'created': created_at, '~from': f'{get_shard()}:Column:Snowflake://production.esikmo/igloo/block1',
                 '~to': f'{get_shard()}:Description:Snowflake://production.esikmo/igloo/block1/_user_description'},
                {'created': created_at, '~from': {'~label': 'Table', 'key': 'Snowflake://production.esikmo/igloo'},
                 '~to': {'~label': 'Description', 'key': 'Snowflake://production.esikmo/igloo/_other_description'}},
                {'created': created_at, '~from': {'~label': 'Table', 'key': 'Snowflake://production.esikmo/igloo'},
                 '~to': {'~label': 'Description', 'key': 'Snowflake://production.esikmo/igloo/_user_description'}}],
            VertexTypes.Description.value: [
                {'description': 'won', 'key': 'Snowflake://production.esikmo/igloo/block1/_user_description',
                 'description_source': 'user'},
                {'description': 'super ' 'cool', 'key': 'Snowflake://production.esikmo/igloo/_other_description',
                 'description_source': 'other'},
                {'description': "it's " 'cool', 'key': 'Snowflake://production.esikmo/igloo/_user_description',
                 'description_source': 'user'}],
            EdgeTypes.Generates.value: [
                {'created': created_at, '~from': {'~label': 'Application', 'key': 'eskimo'},
                 '~to': {'~label': 'Table', 'key': 'Snowflake://production.esikmo/igloo'}},
                {'created': created_at, '~from': {'~label': 'Application', 'key': 'eskimo'},
                 '~to': {'~label': 'Table', 'key': 'Snowflake://production.esikmo/floes'}}],
        })
        actual = GetGraph.table_entities(
            table_data=table_data, created_at=created_at, g=None)
        # make the diff a little better
        self.assertDictEqual(_transform_dict(expected, transform_key=attrgetter('label')),
                             _transform_dict(actual, transform_key=attrgetter('label')))

    def test_table_entities_app_prefix(self, fetch_existing: Any) -> None:
        table_data = [Table(database='Snowflake', cluster='production', schema='esikmo', name='igloo', columns=[],
                            table_writer=Application(id='eskimo'))]

        def side_effect(*args: Any, existing: EXISTING, **kwargs: Any) -> None:
            _FetchExisting._fake_into_existing_vertexes_for_testing(
                _existing=existing, _type=VertexTypes.Application, id='app-eskimo', key='app-eskimo')

        fetch_existing.table_entities.side_effect = side_effect

        created_at = datetime.datetime(2020, 5, 27, 10, 50, 50, 924185)
        expected = _create_expected({
            EdgeTypes.Generates.value: [
                {'created': created_at, '~from': {'~label': 'Application', 'key': 'app-eskimo'},
                 '~to': {'~label': 'Table', 'key': 'Snowflake://production.esikmo/igloo'}},
            ],
        })
        actual = GetGraph.table_entities(table_data=table_data, created_at=created_at, g=None)
        # make the diff a little better, and only look at the expected ones
        self.assertDictEqual(dict((k.label, v) for k, v in expected.items()),
                             dict((k.label, actual[k]) for k, v in expected.items()))

    def test_table_entities_app_owner(self, fetch_existing: Any) -> None:
        table_data = [Table(
            database='Snowflake', cluster='production', schema='esikmo', name='igloo', columns=[],
            table_writer=Application(id='eskimo'),
        )]

        def side_effect(*args: Any, existing: EXISTING, **kwargs: Any) -> None:
            _FetchExisting._fake_into_existing_vertexes_for_testing(
                _existing=existing, _type=VertexTypes.User, user_id='eskimo', key='eskimo')

        fetch_existing.table_entities.side_effect = side_effect

        created_at = datetime.datetime(2020, 5, 27, 10, 50, 50, 924185)
        expected = _create_expected({
            EdgeTypes.Owner.value: [
                {'created': created_at, '~from': {'~label': 'Table', 'key': 'Snowflake://production.esikmo/igloo'},
                 '~to': {'~label': 'User', 'key': 'eskimo'}}],
        })
        actual = GetGraph.table_entities(
            table_data=table_data, created_at=created_at, g=None)
        # make the diff a little better, and only look at the expected ones
        self.assertDictEqual(dict((k.label, v) for k, v in expected.items()),
                             dict((k.label, actual[k]) for k, v in expected.items()))

    def test_user_entities(self, fetch_existing: Any) -> None:
        user_data = [
            User(email='test1@test.com', user_id='test1', first_name="first_name", last_name="last_name",
                 full_name="full_name", display_name="display_name", is_active=True,
                 github_username="github_username", team_name="team_name", slack_id="slack_id",
                 employee_type="employee_type", manager_fullname="manager_fullname", manager_email="manager_email",
                 manager_id="manager_id", role_name="role_name", profile_url="profile_url")]
        expected = _create_expected({
            VertexTypes.User.value: [
                {'display_name': 'display_name', 'email': 'test1@test.com', 'employee_type': 'employee_type',
                 'first_name': 'first_name', 'full_name': 'full_name', 'github_username': 'github_username',
                 'is_active': True, 'key': 'test1', 'last_name': 'last_name', 'manager_email': 'manager_email',
                 'manager_fullname': 'manager_fullname', 'manager_id': 'manager_id', 'profile_url': 'profile_url',
                 'role_name': 'role_name', 'slack_id': 'slack_id', 'team_name': 'team_name', 'user_id': 'test1'},
            ],
        })

        actual = GetGraph.user_entities(user_data=user_data, g=None)
        # make the diff a little better
        self.assertDictEqual(dict((k.label, v) for k, v in expected.items()),
                             dict((k.label, v) for k, v in actual.items()))

    def test_app_entities(self, fetch_existing: Any) -> None:
        app_data = [Application(application_url="wais://", description="description", id="college", name="essay")]
        expected = _create_expected({
            VertexTypes.Application.value: [
                {'application_url': 'wais://', 'description': 'description', 'id': 'college', 'key': 'college',
                 'name': 'essay'},
            ],
        })
        actual = GetGraph.app_entities(app_data=app_data, g=None)
        # make the diff a little better
        self.assertDictEqual(dict((k.label, v) for k, v in expected.items()),
                             dict((k.label, v) for k, v in actual.items()))

    def test_duplicates_ok(self, fetch_existing: Any) -> None:
        table_data = [
            Table(database='Snowflake', cluster='production', schema='esikmo', name='igloo', columns=[]),
            Table(database='Snowflake', cluster='production', schema='esikmo', name='electric-bugaloo', columns=[])]

        created_at = datetime.datetime(2020, 5, 27, 10, 50, 50, 924185)
        expected = _create_expected({
            # only one of these, from here:
            VertexTypes.Database.value: [
                {'key': 'database://Snowflake', 'name': 'Snowflake'},
            ],
            EdgeTypes.Cluster.value: [
                {'created': created_at, '~from': {'~label': 'Database', 'key': 'database://Snowflake'},
                 '~to': {'~label': 'Cluster', 'key': 'Snowflake://production'}},
            ],
            VertexTypes.Cluster.value: [
                {'key': 'Snowflake://production', 'name': 'production'},
            ],
            EdgeTypes.Schema.value: [
                {'created': created_at, '~from': {'~label': 'Cluster', 'key': 'Snowflake://production'},
                 '~to': {'~label': 'Schema', 'key': 'Snowflake://production.esikmo'}},
            ],
            VertexTypes.Schema.value: [
                {'key': 'Snowflake://production.esikmo', 'name': 'esikmo'},
            ],
            # ...to here.
            EdgeTypes.Table.value: [
                {'created': created_at, '~from': {'~label': 'Schema', 'key': 'Snowflake://production.esikmo'},
                 '~to': {'~label': 'Table', 'key': 'Snowflake://production.esikmo/electric-bugaloo'}},
                {'created': created_at, '~from': {'~label': 'Schema', 'key': 'Snowflake://production.esikmo'},
                 '~to': {'~label': 'Table', 'key': 'Snowflake://production.esikmo/igloo'}},
            ],
            VertexTypes.Table.value: [
                {'is_view': False, 'key': 'Snowflake://production.esikmo/electric-bugaloo', 'name': 'electric-bugaloo'},
                {'is_view': False, 'key': 'Snowflake://production.esikmo/igloo', 'name': 'igloo'},
            ],
            EdgeTypes.LastUpdatedAt.value: [
                {'created': created_at,
                 '~from': {'~label': 'Table', 'key': 'Snowflake://production.esikmo/electric-bugaloo'},
                 '~to': {'~label': 'Updatedtimestamp', 'key': 'Snowflake://production.esikmo/electric-bugaloo'}},
                {'created': created_at, '~from': {'~label': 'Table', 'key': 'Snowflake://production.esikmo/igloo'},
                 '~to': {'~label': 'Updatedtimestamp', 'key': 'Snowflake://production.esikmo/igloo'}},
            ],
            VertexTypes.Updatedtimestamp.value: [
                {'key': 'amundsen_updated_timestamp', 'latest_timestamp': created_at},
                {'key': 'Snowflake://production.esikmo/electric-bugaloo', 'latest_timestamp': created_at},
                {'key': 'Snowflake://production.esikmo/igloo', 'latest_timestamp': created_at},
            ],
        })

        actual = GetGraph.table_entities(table_data=table_data, created_at=created_at, g=None)
        # make the diff a little better
        self.assertDictEqual(dict((k.label, v) for k, v in expected.items()),
                             dict((k.label, v) for k, v in actual.items()))

    def test_duplicates_explode(self, fetch_existing: Any) -> None:
        user_data = [Fixtures.next_user(user_id='u'), Fixtures.next_user(user_id='u'), Fixtures.next_user(user_id='u')]
        # with self.assertRaisesRegex(AssertionError, 'we already have a .*id=User:u that is different: '):
        with self.assertLogs('amundsen_gremlin.neptune_bulk_loader.gremlin_model_converter', level='INFO') as cm:
            GetGraph.user_entities(user_data=user_data, g=None)
        self.assertTrue(
            len(cm.output) == 2
            and all(f'we already have a type: User, id={get_shard()}:User:u that is different' in line for line in cm.output),
            f'expected message in {cm.output}')


@pytest.mark.roundtrip
class TestGetGraphRoundTrip(unittest.TestCase):
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

    def _bulk_load_entities_successfully(
            self, *, entities: Mapping[GraphEntityType, Mapping[str, GraphEntity]], **kwargs: Any) -> None:
        self.bulk_loader.bulk_load_entities(
            entities=entities, raise_if_failed=True, object_prefix=f'{{now}}/{get_shard()}', **kwargs)

    def test_table_entities(self) -> None:
        app_data = [Application(id='eskimo')]
        table_data = [Table(
            database='Snowflake', cluster='production', schema='esikmo', name='igloo',
            description='''it's cool''',
            programmatic_descriptions=[ProgrammaticDescription(text='super cool', source='other')],
            table_writer=Application(id='eskimo'),
            columns=[
                Column(name='block1', col_type='ice', sort_order=1, description='won'),
                Column(name='block2', col_type='ice', sort_order=2),
            ],
            tags=[Tag(tag_name='Kewl', tag_type='default')],
        )]

        created_at = datetime.datetime(2020, 5, 27, 10, 50, 50)  # omit the precision here, we're roundtripping
        entities1 = GetGraph(created_at=created_at, g=self.neptune_graph_traversal_source_factory()).\
            add_app_entities(app_data).add_table_entities(table_data).complete()
        self._bulk_load_entities_successfully(entities=entities1)

        created_at = datetime.datetime(2020, 5, 27, 10, 50, 50, 924185) + datetime.timedelta(seconds=10, days=1)
        entities2 = GetGraph(created_at=created_at, g=self.neptune_graph_traversal_source_factory()).\
            add_app_entities(app_data).add_table_entities(table_data).complete()

        # Updatedtimestamp vertex should change
        self.assertDictEqual(dict((k.label, v) for k, v in entities1.items() if k.label != 'Updatedtimestamp'),
                             dict((k.label, v) for k, v in entities2.items() if k.label != 'Updatedtimestamp'))

# TODO: redo this test
    def test_expire_others(self) -> None:
        pass


class TestGetGraphMisc(unittest.TestCase):
    def test_possible_application_names_application_key(self) -> None:
        self.assertSetEqual({'app-foo', 'foo'}, set(possible_application_names_application_key('foo')))
        self.assertSetEqual({'app-foo', 'foo'}, set(possible_application_names_application_key('app-foo')))
        self.assertSetEqual({'app-foo', 'foo', 'app-foo-devel', 'foo-devel'},
                            set(possible_application_names_application_key('foo-devel')))
        self.assertSetEqual({'app-foo', 'foo', 'app-foo-development', 'foo-development'},
                            set(possible_application_names_application_key('foo-development')))
        self.assertSetEqual({'app-foo', 'foo', 'app-foo-stage', 'foo-stage'},
                            set(possible_application_names_application_key('foo-stage')))
        self.assertSetEqual({'app-foo', 'foo', 'app-foo-staging', 'foo-staging'},
                            set(possible_application_names_application_key('foo-staging')))
        self.assertSetEqual({'app-foo', 'foo', 'app-foo-prod', 'foo-prod'},
                            set(possible_application_names_application_key('foo-prod')))
        self.assertSetEqual({'app-foo', 'foo', 'app-foo-production', 'foo-production'},
                            set(possible_application_names_application_key('foo-production')))


K = TypeVar('K', bound=Hashable)
V = TypeVar('V')
K2 = TypeVar('K2', bound=Hashable)
V2 = TypeVar('V2')


def _transform_dict(  # noqa: C901
        mapping: Mapping[K, V], *, if_key: Optional[Callable[[K], bool]] = None,
        if_value: Optional[Callable[[V], bool]] = None, if_item: Optional[Callable[[K, V], bool]] = None,
        transform_key: Optional[Callable[[K], K2]] = None, transform_value: Optional[Callable[[V], V2]] = None,
        transform_item: Optional[Callable[[K, V], Tuple[K2, V2]]] = None) -> Union[Dict[K, V], Dict[K2, V], Dict[K, V2], Dict[K2, V2]]:
    assert len([c for c in (if_key, if_value, if_item) if c is not None]) <= 1, \
        f'expected exactly at most one of if_key, if_value, or if_item'
    assert len([c for c in (transform_key, transform_value, transform_item) if c is not None]) <= 1, \
        f'expected exactly at most one of transform_key, transform_value, or transform_item'

    # I couldn't make mypy like the overloading
    if transform_key is not None:
        if if_key is not None:
            return dict([(transform_key(k), v) for k, v in mapping.items() if if_key(k)])
        elif if_value is not None:
            return dict([(transform_key(k), v) for k, v in mapping.items() if if_value(v)])
        elif if_item is not None:
            return dict([(transform_key(k), v) for k, v in mapping.items() if if_item(k, v)])
        else:
            return dict([(transform_key(k), v) for k, v in mapping.items()])
    elif transform_value is not None:
        if if_key is not None:
            return dict([(k, transform_value(v)) for k, v in mapping.items() if if_key(k)])
        elif if_value is not None:
            return dict([(k, transform_value(v)) for k, v in mapping.items() if if_value(v)])
        elif if_item is not None:
            return dict([(k, transform_value(v)) for k, v in mapping.items() if if_item(k, v)])
        else:
            return dict([(k, transform_value(v)) for k, v in mapping.items()])
    elif transform_item is not None:
        if if_key is not None:
            return dict([transform_item(k, v) for k, v in mapping.items() if if_key(k)])
        elif if_value is not None:
            return dict([transform_item(k, v) for k, v in mapping.items() if if_value(v)])
        elif if_item is not None:
            return dict([transform_item(k, v) for k, v in mapping.items() if if_item(k, v)])
        else:
            return dict([transform_item(k, v) for k, v in mapping.items()])
    else:
        if if_key is not None:
            return dict([(k, v) for k, v in mapping.items() if if_key(k)])
        elif if_value is not None:
            return dict([(k, v) for k, v in mapping.items() if if_value(v)])
        elif if_item is not None:
            return dict([(k, v) for k, v in mapping.items() if if_item(k, v)])
        else:
            return dict([(k, v) for k, v in mapping.items()])


VERTEX_OR_EDGE_TYPE = TypeVar('VERTEX_OR_EDGE_TYPE', bound=Union[VertexType, EdgeType])


def _label_not_in(*labels: str) -> Callable[[VERTEX_OR_EDGE_TYPE], bool]:
    def not_in(_type: VERTEX_OR_EDGE_TYPE) -> bool:
        return _type is not None and _type.label not in labels
    return not_in
