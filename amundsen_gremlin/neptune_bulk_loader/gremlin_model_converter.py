# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import datetime
import logging
from collections import defaultdict
from functools import lru_cache
from typing import (
    Any, Callable, FrozenSet, Iterable, List, Mapping, MutableMapping,
    NamedTuple, NewType, Optional, Sequence, Tuple, Union, cast
)

from amundsen_common.models.table import Application, Column, Table
from amundsen_common.models.user import User
from gremlin_python.process.graph_traversal import GraphTraversalSource, __
from gremlin_python.process.traversal import Column as MapColumn
from gremlin_python.process.traversal import T

from amundsen_gremlin.gremlin_model import (
    EdgeType, EdgeTypes, GremlinCardinality, MagicProperties, Property,
    VertexType, VertexTypes, WellKnownProperties
)
from amundsen_gremlin.gremlin_shared import (  # noqa: F401
    append_traversal, make_cluster_uri, make_column_statistic_uri,
    make_column_uri, make_database_uri, make_description_uri, make_schema_uri,
    make_table_uri
)
from amundsen_gremlin.utils.streams import chunk

LOGGER = logging.getLogger(__name__)

EXISTING_KEY = FrozenSet[Tuple[str, str]]
EXISTING = NewType('EXISTING',
                   Mapping[Union[VertexType, EdgeType], MutableMapping[EXISTING_KEY, Mapping[str, Any]]])
ENTITIES = NewType('ENTITIES', Mapping[Union[VertexType, EdgeType], MutableMapping[str, Mapping[str, Any]]])


def new_entities() -> ENTITIES:
    return cast(ENTITIES, defaultdict(dict))


def new_existing() -> EXISTING:
    return cast(EXISTING, defaultdict(dict))


def _get_existing_key_from_entity(_entity: Mapping[str, Any]) -> EXISTING_KEY:
    """
    Used for testing.
    """
    _entity = dict(_entity)
    label: str = _entity.pop(MagicProperties.LABEL.value.name)
    assert isinstance(label, str)  # appease the types
    return _get_existing_key(_type=label, **_entity)


def _get_existing_key(_type: Union[VertexType, EdgeType, VertexTypes, EdgeTypes, str], **_entity: Any) -> EXISTING_KEY:
    """
    Maybe this should be a part of EdgeType and VertexType.  But, this function certainly shouldn't be used
    away from EXISTING (or testing)
    """
    if isinstance(_type, str):
        vertex_type = VertexTypes.by_label().get(_type)
        edge_type = EdgeTypes.by_label().get(_type)
        assert bool(vertex_type) != bool(edge_type), \
            f'expected exactly one of VertexTypes or EdgeTypes to match {_type}'
        vertex_or_edge_type = vertex_type or edge_type
        assert vertex_or_edge_type is not None  # appease mypy
        _type = vertex_or_edge_type
        assert isinstance(_type, (VertexTypes, EdgeTypes))
    if isinstance(_type, (VertexTypes, EdgeTypes)):
        _type = _type.value
    assert isinstance(_type, (VertexType, EdgeType))
    key_properties = _get_key_properties(_type)
    # this eliding (the created and label fields) feels icky, but makes sense.
    if isinstance(_type, EdgeType):
        key_properties = key_properties.difference({WellKnownProperties.Created.value})
    if isinstance(_type, VertexType):
        key_properties = key_properties.difference({WellKnownProperties.TestShard.value})
    key_properties = key_properties.difference({MagicProperties.LABEL.value})
    assert WellKnownProperties.Created.value not in key_properties
    assert MagicProperties.LABEL.value not in key_properties
    return frozenset([(p.name, p.format(_entity.get(p.name))) for p in key_properties])


@lru_cache(maxsize=len(list(VertexTypes)) + len(list(EdgeTypes)) + 100)
def _get_key_properties(_type: Union[VertexType, EdgeType]) -> FrozenSet[Property]:
    assert isinstance(_type, (EdgeType, VertexType))
    return frozenset([_type.properties_as_map()[n] for n in _discover_parameters(_type.id_format)])


def _discover_parameters(format_string: str) -> FrozenSet[str]:
    """
    use this to discover what the parameters to a format string are
    """
    parameters: FrozenSet[str] = frozenset()
    while True:
        try:
            format_string.format(**dict((k, '') for k in parameters))
            return parameters
        except KeyError as e:
            updated = parameters.union(set(e.args))
            assert updated != parameters
            parameters = updated


def date_string_to_date(a_date: str) -> datetime.date:
    return datetime.datetime.strptime(a_date, '%Y-%m-%d').date()


class TableUris(NamedTuple):
    database: str
    cluster: str
    schema: str
    table: str

    @staticmethod
    def get(*, database: str, cluster: str, schema: str, table: str) -> "TableUris":
        database_uri = make_database_uri(database_name=database)
        cluster_uri = make_cluster_uri(database_uri=database_uri, cluster_name=cluster)
        schema_uri = make_schema_uri(cluster_uri=cluster_uri, schema_name=schema)
        table_uri = make_table_uri(schema_uri=schema_uri, table_name=table)
        return TableUris(database=database_uri, cluster=cluster_uri, schema=schema_uri, table=table_uri)


HISTORICAL_APP_PREFIX = 'app-'
ENVIRONMENT_APP_SUFFIXES = frozenset(['-development', '-devel', '-staging', '-stage', '-production', '-prod'])


def possible_application_names_application_key(app_key: str) -> Iterable[str]:
    # get both the app- and not
    app_keys = [app_key]
    if app_key.startswith(HISTORICAL_APP_PREFIX):
        app_keys.append(app_key[len(HISTORICAL_APP_PREFIX):])
    else:
        app_keys.append(f'{HISTORICAL_APP_PREFIX}{app_key}')

    for suffix in ENVIRONMENT_APP_SUFFIXES:
        if app_key.endswith(suffix):
            without = [_[:-len(suffix)] for _ in app_keys]
            app_keys.extend(without)
            break

    return tuple(app_keys)


def possible_existing_keys_for_application_key(*app_keys: str) -> FrozenSet[EXISTING_KEY]:
    return frozenset([_get_existing_key(VertexTypes.Application, key=key)
                      for app_key in app_keys for key in possible_application_names_application_key(app_key)])


def possible_vertex_ids_for_application_key(*app_keys: str) -> FrozenSet[str]:
    return frozenset([
        VertexTypes.Application.value.id(**dict(key)) for key in possible_existing_keys_for_application_key(*app_keys)])


def ensure_edge_type(edge_type: Union[str, EdgeTypes, EdgeType]) -> EdgeType:
    if isinstance(edge_type, str):
        edge_type = EdgeTypes.by_label()[edge_type].value
    if isinstance(edge_type, EdgeTypes):
        edge_type = edge_type.value
    assert isinstance(edge_type, EdgeType)
    return edge_type


def ensure_vertex_type(vertex_type: Union[str, VertexTypes, VertexType]) -> VertexType:
    if isinstance(vertex_type, str):
        vertex_type = VertexTypes.by_label()[vertex_type].value
    if isinstance(vertex_type, VertexTypes):
        vertex_type = vertex_type.value
    assert isinstance(vertex_type, VertexType)
    return vertex_type


class _FetchExisting:
    @classmethod
    def _fake_into_existing_edges_for_testing(cls, _existing: EXISTING, _type: Union[EdgeType, EdgeTypes], _from: str,
                                              _to: str, **entity: Any) -> Mapping[str, Any]:
        _type = ensure_edge_type(_type)
        _entity = _type.create(**entity, **{
            MagicProperties.LABEL.value.name: _type.label,
            MagicProperties.FROM.value.name: _from,
            MagicProperties.TO.value.name: _to,
        })
        _key = _get_existing_key(_type=_type, **_entity)
        assert _key not in _existing[_type]
        _existing[_type][_key] = _entity
        return _entity

    @classmethod
    def _fake_into_existing_vertexes_for_testing(cls, _existing: EXISTING, _type: Union[VertexType, VertexTypes],
                                                 **entity: Any) -> Mapping[str, Any]:
        _type = ensure_vertex_type(_type)
        _entity = _type.create(** entity, **{
            MagicProperties.LABEL.value.name: _type.label,
        })
        _key = _get_existing_key(_type=_type, **_entity)
        assert _key not in _existing[_type]
        _existing[_type][_key] = _entity
        return _entity

    @classmethod  # noqa: C901
    def _honor_cardinality_once(cls, _property: Property, value: Any) -> Any:
        # use the types to figure out if we should take the element instead
        if _property.cardinality == GremlinCardinality.single or _property.cardinality is None:
            # is this the most general type?
            if isinstance(value, Sequence):
                assert len(value) <= 1, f'single cardinality property has more than one value! {value}'
                value = value[0] if value else None
            if value is not None:
                _property.type.value.is_allowed(value)
            return value
        elif _property.cardinality == GremlinCardinality.list:
            # is this the most general type?
            if value is None:
                value = ()
            elif isinstance(value, Iterable) and not isinstance(value, tuple):
                value = tuple(value)
            for e in value:
                _property.type.value.is_allowed(e)
            return value
        elif _property.cardinality == GremlinCardinality.set:
            # is this the most general type?
            if value is None:
                value = frozenset()
            elif isinstance(value, Iterable) and not isinstance(value, FrozenSet):
                value = frozenset(value)
            for e in value:
                _property.type.value.is_allowed(e)
            return value
        raise AssertionError('never')

    @classmethod
    def _honor_cardinality(cls, _type: Union[VertexType, EdgeType], **entity: Any) -> Mapping[str, Any]:
        _properties = _type.properties_as_map()
        result = dict()
        for k, v in entity.items():
            if not _properties.get(k):
                LOGGER.error(f'Trying to honor cardinality for property {k} which isnt allowed for {_type.label}')
                continue
            result[k] = cls._honor_cardinality_once(_properties[k], v)
        return result

    @classmethod
    def _into_existing(cls, value_maps: Sequence[Union[Mapping[Any, Any], Sequence[Any]]], existing: EXISTING) -> None:
        """
        value_map for an edge should be the result of .union(__.outV().id(), __.valueMap(True), __.inV().id()).fold()
        value_map for a vertex should be the result of valueMap(True)
        """
        assert all(isinstance(e, (Mapping, Sequence)) for e in value_maps)
        edge_value_maps = [e for e in value_maps if isinstance(e, Sequence)]
        vertex_value_maps = [e for e in value_maps if isinstance(e, Mapping)]
        assert len(value_maps) == len(edge_value_maps) + len(vertex_value_maps)

        for _from, entity, _to in edge_value_maps:
            entity = dict(entity)
            _type = EdgeTypes.by_label()[entity.pop(T.label)].value
            _id = entity.pop(T.id)
            # clear out the other special values.  eventually we'll be able to ask for just the id and label, but that's
            # not supported in Neptune (you can only do valueMap(True))
            for v in iter(T):
                entity.pop(v, None)
            _entity = _type.create(**entity, **{
                MagicProperties.LABEL.value.name: _type.label,
                MagicProperties.ID.value.name: _id,
                MagicProperties.FROM.value.name: _from,
                MagicProperties.TO.value.name: _to,
            })
            _key = _get_existing_key(_type=_type, **_entity)
            # should we expect only one? things like the CLUSTER, and SCHEMA will duplicate
            if _key in existing[_type]:
                if existing[_type][_key] != _entity:
                    LOGGER.info(f'we already have a type: {_type.label}, id={_id} that is different: '
                                f'{existing[_type][_key]} != {_entity}')
            else:
                # should the magic properties go in here too?  It might be nicer to not, but is convenient
                existing[_type][_key] = _entity

        for entity in vertex_value_maps:
            entity = dict(entity)
            _type = VertexTypes.by_label()[entity.pop(T.label)].value
            _id = entity.pop(T.id)
            # clear out the other special values.  eventually we'll be able to ask for just the id and label, but that's
            # not supported in Neptune (you can only do valueMap(True))
            for v in iter(T):
                entity.pop(v, None)
            _entity = _type.create(**cls._honor_cardinality(_type, **entity), **{
                MagicProperties.LABEL.value.name: _type.label,
                MagicProperties.ID.value.name: _id,
            })
            _key = _get_existing_key(_type=_type, **_entity)
            # should we expect only one? things like the CLUSTER, and SCHEMA will duplicate
            if _key in existing[_type]:
                if existing[_type][_key] != _entity:
                    LOGGER.error(f'we already have a type: {_type.label}, id={_id} that is different: '
                                 f'{existing[_type][_key]} != {_entity}')
            else:
                # should the magic properties go in here too?  It might be nicer to not, but is convenient
                existing[_type][_key] = _entity

    @classmethod
    def table_entities(cls, *, _g: GraphTraversalSource, table_data: List[Table], existing: EXISTING) -> None:

        all_tables_ids = list(set([
            VertexTypes.Table.value.id(key=TableUris.get(
                database=t.database, cluster=t.cluster, schema=t.schema, table=t.name).table)
            for t in table_data]))

        all_owner_ids = list(set([VertexTypes.User.value.id(key=key)
                                  for key in [t.table_writer.id for t in table_data if t.table_writer is not None]]))
        all_application_ids = list(set(list(possible_vertex_ids_for_application_key(
            *[t.table_writer.id for t in table_data if t.table_writer is not None]))))

        # chunk these since 100,000s seems to choke
        for tables_ids in chunk(all_tables_ids, 1000):
            LOGGER.info(f'fetching for tables: {tables_ids}')
            # fetch database -> cluster -> schema -> table links
            g = _g.V(tuple(tables_ids)).as_('tables')
            g = g.coalesce(__.inE(EdgeTypes.Table.value.label).dedup().fold()).as_(EdgeTypes.Table.name)
            g = g.coalesce(__.unfold().outV().hasLabel(VertexTypes.Schema.value.label).
                           inE(EdgeTypes.Schema.value.label).dedup().
                           fold()).as_(EdgeTypes.Schema.name)
            g = g.coalesce(__.unfold().outV().hasLabel(VertexTypes.Cluster.value.label).
                           inE(EdgeTypes.Cluster.value.label).dedup().
                           fold()).as_(EdgeTypes.Cluster.name)

            # fetch table <- links
            for t in (EdgeTypes.BelongToTable, EdgeTypes.Generates, EdgeTypes.Tag):
                g = g.coalesce(
                    __.select('tables').inE(t.value.label).fold()).as_(t.name)

            # fetch table -> column et al links
            for t in (EdgeTypes.Column, EdgeTypes.Description, EdgeTypes.LastUpdatedAt,
                      EdgeTypes.Source, EdgeTypes.Stat):
                g = g.coalesce(
                    __.select('tables').outE(t.value.label).fold()).as_(t.name)

            # TODO: add owners, watermarks, last timestamp existing, source
            aliases = set([t.name for t in (
                EdgeTypes.Table, EdgeTypes.Schema, EdgeTypes.Cluster, EdgeTypes.BelongToTable, EdgeTypes.Generates,
                EdgeTypes.Tag, EdgeTypes.Column, EdgeTypes.Description, EdgeTypes.LastUpdatedAt,
                EdgeTypes.Source, EdgeTypes.Stat)])
            g = g.select(*aliases).unfold().select(MapColumn.values).unfold()
            g = g.local(__.union(__.outV().id(), __.valueMap(True), __.inV().id()).fold())
            cls._into_existing(g.toList(), existing)

            cls._column_entities(_g=_g, tables_ids=tables_ids, existing=existing)

        # fetch Application, User
        for ids in chunk(list(set(all_application_ids + all_owner_ids)), 5000):
            LOGGER.info(f'fetching for application/owners: {ids}')
            g = _g.V(ids).valueMap(True)
            cls._into_existing(g.toList(), existing)

    @classmethod
    def _column_entities(cls, *, _g: GraphTraversalSource, tables_ids: Iterable[str], existing: EXISTING) -> None:
        # fetch database -> cluster -> schema -> table links
        g = _g.V(tuple(tables_ids))
        g = g.outE(EdgeTypes.Column.value.label)
        g = g.inV().hasLabel(VertexTypes.Column.value.label).as_('columns')

        # fetch column -> links (no Stat)
        for t in [EdgeTypes.Description]:
            g = g.coalesce(__.select('columns').outE(t.value.label).fold()).as_(t.name)

        g = g.select(EdgeTypes.Description.name).unfold()
        g = g.local(__.union(__.outV().id(), __.valueMap(True), __.inV().id()).fold())
        cls._into_existing(g.toList(), existing)

    @classmethod
    def expire_connections_for_other(cls, *, _g: GraphTraversalSource, vertex_type: VertexType, keys: FrozenSet[str],
                                     existing: EXISTING) -> None:
        # V().has(label, 'key', P.without(keys)) is more intuitive but doesn't scale, so instead just find all those
        g = _g.V().hasLabel(vertex_type.label).where(__.bothE())
        g = g.values(WellKnownProperties.Key.value.name)
        all_to_expire_keys = set(g.toList()).difference(keys)

        # TODO: when any vertex ids that need something besides key
        all_to_expire = set(vertex_type.id(key=key) for key in all_to_expire_keys)

        for to_expire in chunk(all_to_expire, 1000):
            g = _g.V(tuple(to_expire)).bothE()
            g = g.local(__.union(__.outV().id(), __.valueMap(True), __.inV().id()).fold())
            cls._into_existing(g.toList(), existing)


class _GetGraph:
    @classmethod
    def expire_previously_existing(cls, *, edge_types: Sequence[Union[EdgeTypes, EdgeType]], entities: ENTITIES,
                                   existing: EXISTING) -> None:
        _edge_types = [e.value if isinstance(e, EdgeTypes) else e for e in edge_types]
        assert all(isinstance(e, EdgeType) for e in _edge_types), \
            f'expected all EdgeTypes or EdgeType: {edge_types}'

        for edge_type in _edge_types:
            for entity in existing[edge_type].values():

                entity_id = entity[MagicProperties.ID.value.name]
                if entity_id in entities[edge_type]:
                    continue

                del entities[edge_type][entity_id]

    @classmethod
    def _create(cls, _type: Union[VertexTypes, VertexType, EdgeTypes, EdgeType], _entities: ENTITIES,
                _existing: EXISTING, **_kwargs: Any) -> Mapping[str, Any]:
        if isinstance(_type, (VertexTypes, EdgeTypes)):
            _type = _type.value
        assert isinstance(_type, (VertexType, EdgeType))

        # Let's prefer the new properties unless it's part of the the id properties (e.g. Created)
        _existing_key = _get_existing_key(_type, **_kwargs)
        if _existing_key in _existing[_type]:
            names = frozenset(p.name for p in _get_key_properties(_type))
            _kwargs.update((k, v) for k, v in _existing[_type][_existing_key].items() if k in names)
            # need to do this after that update, otherwise we'll miss out on crucial properties when generating ~id
            _entity = _type.create(**_kwargs)
        else:
            _entity = _type.create(**_kwargs)
            # also put this in _existing.  Say, we're creating a Column or Table and a subsequence Description expects
            # to find it.  (TODO: This isn't perfect, it will miss tables_by_app, and neighbors_by_capability)
            _existing[_type][_existing_key] = _entity

        _id = _entity.get(MagicProperties.ID.value.name, None)
        if _id in _entities[_type]:
            # it'd be nice to assert _id not in _entities[_type], but we generate duplicates (e.g. Database, Cluster,
            # Schema, and their links) so let's at least ensure we're not going to be surprised with a different result
            # TODO: reenable this after we figure out why these conflict
            # assert _entities[_type][_id] == _entity, \
            if _entities[_type][_id] != _entity:
                LOGGER.info(f'we already have a type: {_type.label}, id={_id} that is different: '
                            f'{_entities[_type][_id]} != {_entity}')
        else:
            _entities[_type][_id] = _entity
        return _entities[_type][_id]

    @classmethod
    def table_metric(cls, table: Table) -> int:
        """
        :returns a number like the number of vertexes that would be added due to this table
        """
        return sum((2, 1 if table.description is not None else 0,
                    len(table.programmatic_descriptions or ()), len(table.programmatic_descriptions or ()),
                    len(table.tags or ()), sum(map(cls._column_metric, table.columns))))

    @classmethod
    def table_entities(cls, *, table_data: List[Table], entities: ENTITIES, existing: EXISTING,  # noqa: C901
                       created_at: datetime.datetime) -> None:
        """
        existing: must cover exactly the set of data.  (previously existing edges will be expired herein, and possibly
        otherwise duplicate edges will be created)
        """

        for table in table_data:
            uris = TableUris.get(database=table.database, cluster=table.cluster, schema=table.schema, table=table.name)

            database = cls._create(
                VertexTypes.Database, entities, existing, name=table.database, key=uris.database)

            cluster = cls._create(VertexTypes.Cluster, entities, existing, name=table.cluster, key=uris.cluster)
            cls._create(EdgeTypes.Cluster, entities, existing, created=created_at, **{
                MagicProperties.FROM.value.name: database[MagicProperties.ID.value.name],
                MagicProperties.TO.value.name: cluster[MagicProperties.ID.value.name]})

            schema = cls._create(VertexTypes.Schema, entities, existing, name=table.schema, key=uris.schema)
            cls._create(EdgeTypes.Schema, entities, existing, created=created_at, **{
                MagicProperties.FROM.value.name: cluster[MagicProperties.ID.value.name],
                MagicProperties.TO.value.name: schema[MagicProperties.ID.value.name]})

            table_vertex = cls._create(VertexTypes.Table, entities, existing, name=table.name, key=uris.table,
                                       is_view=table.is_view)
            cls._create(EdgeTypes.Table, entities, existing, created=created_at, **{
                MagicProperties.FROM.value.name: schema[MagicProperties.ID.value.name],
                MagicProperties.TO.value.name: table_vertex[MagicProperties.ID.value.name]})

            if table.table_writer:
                cls._application_entities(app_key=table.table_writer.id, table=table_vertex, entities=entities,
                                          existing=existing, created_at=created_at)

            if table.description is not None:
                cls._description_entities(
                    subject_uri=table_vertex['key'], to_vertex_id=table_vertex[MagicProperties.ID.value.name],
                    source='user', entities=entities, existing=existing, created_at=created_at,
                    description=table.description)

            for description in table.programmatic_descriptions:
                cls._description_entities(
                    subject_uri=table_vertex['key'], to_vertex_id=table_vertex[MagicProperties.ID.value.name],
                    source=description.source, entities=entities, existing=existing, created_at=created_at,
                    description=description.text)
                # TODO: need to call expire source != 'user' description links after

            # create tags
            for tag in table.tags:
                vertex = cls._create(VertexTypes.Tag, entities, existing, key=tag.tag_name, **vars(tag))
                cls._create(EdgeTypes.Tag, entities, existing, created=created_at, **{
                    MagicProperties.FROM.value.name: vertex[MagicProperties.ID.value.name],
                    MagicProperties.TO.value.name: table_vertex[MagicProperties.ID.value.name]})
                # since users can tag these, we shouldn't expire any of them (unlike Description where source
                # distinguishes)

            # update timestamp
            # Amundsen global timestamp
            cls._create(VertexTypes.Updatedtimestamp, entities, existing, key='amundsen_updated_timestamp',
                        latest_timestamp=created_at)
            # Table-specific timestamp
            vertex = cls._create(VertexTypes.Updatedtimestamp, entities, existing, key=table_vertex['key'],
                                 latest_timestamp=created_at)
            cls._create(EdgeTypes.LastUpdatedAt, entities, existing, created=created_at, **{
                MagicProperties.FROM.value.name: table_vertex[MagicProperties.ID.value.name],
                MagicProperties.TO.value.name: vertex[MagicProperties.ID.value.name]})

            cls._column_entities(table_vertex=table_vertex, column_data=table.columns, entities=entities,
                                 existing=existing, created_at=created_at)

    @classmethod
    def _application_entities(cls, *, app_key: str, table: Mapping[str, Mapping[str, Any]], entities: ENTITIES,
                              existing: EXISTING, created_at: datetime.datetime) -> None:
        # use existing to find what Application really exists, which is a bit different than how it's used for edges
        actual_keys = dict([
            (VertexTypes.Application.value.id(**dict(v)), v)
            for v in possible_existing_keys_for_application_key(app_key)])
        actual_keys = dict([(k, v) for k, v in actual_keys.items() if v in existing[VertexTypes.Application.value]])
        if actual_keys:
            vertex_id = list(actual_keys.items())[0][0]
            cls._create(EdgeTypes.Generates, entities, existing, created=created_at, **{
                MagicProperties.FROM.value.name: vertex_id,
                MagicProperties.TO.value.name: table[MagicProperties.ID.value.name]})
            return

        # if app isn't found, the owner may be a user
        actual_keys = dict([(VertexTypes.User.value.id(key=app_key), _get_existing_key(VertexTypes.User, key=app_key))])
        actual_keys = dict([(k, v) for k, v in actual_keys.items() if v in existing[VertexTypes.User.value]])
        if actual_keys:
            vertex_id = list(actual_keys.items())[0][0]
            LOGGER.debug(f'{app_key} is not a real app but it was marked as owner: {table["key"]}')
            cls._create(EdgeTypes.Owner, entities, existing, created=created_at, **{
                MagicProperties.FROM.value.name: table[MagicProperties.ID.value.name],
                MagicProperties.TO.value.name: vertex_id})
            return

        LOGGER.info(f'{app_key} is not a real Application, nor can we find a User to be an Owner for {table["key"]}')

    @classmethod
    def _description_entities(cls, *, description: str, source: str, subject_uri: str,
                              to_vertex_id: str, entities: ENTITIES, existing: EXISTING,
                              created_at: datetime.datetime) -> None:
        vertex = cls._create(VertexTypes.Description, entities, existing,
                             key=make_description_uri(subject_uri=subject_uri, source=source),
                             description=description, description_source=source)
        cls._create(EdgeTypes.Description, entities, existing, created=created_at, **{
            MagicProperties.FROM.value.name: to_vertex_id,
            MagicProperties.TO.value.name: vertex[MagicProperties.ID.value.name]})

    @classmethod
    def _column_metric(cls, column: Column) -> int:
        """
        :returns a number like the number of vertexes that would be added due to this column
        """
        return sum((1, 1 if column.description is not None else 0, len(column.stats or ())))

    @classmethod
    def _column_entities(cls, *, table_vertex: Mapping[str, str], column_data: Sequence[Column], entities: ENTITIES,
                         existing: EXISTING, created_at: datetime.datetime) -> None:

        for column in column_data:
            column_vertex = cls._create(VertexTypes.Column, entities, existing, name=column.name,
                                        key=make_column_uri(table_uri=table_vertex['key'], column_name=column.name),
                                        col_type=column.col_type, sort_order=column.sort_order)
            cls._create(EdgeTypes.Column.value, entities, existing, created=created_at, **{
                MagicProperties.FROM.value.name: table_vertex[MagicProperties.ID.value.name],
                MagicProperties.TO.value.name: column_vertex[MagicProperties.ID.value.name]})

            # Add the description if present
            if column.description is not None:
                cls._description_entities(
                    subject_uri=column_vertex['key'], to_vertex_id=column_vertex[MagicProperties.ID.value.name],
                    source='user', entities=entities, existing=existing, created_at=created_at,
                    description=column.description)

            # Add stats if present
            if column.stats:
                for stat in column.stats:
                    vertex = cls._create(
                        VertexTypes.Stat, entities, existing,
                        key=make_column_statistic_uri(column_uri=column_vertex['key'], statistic_type=stat.stat_type),
                        # stat.stat_val is a str, but some callers seem to put ints in there
                        stat_val=(None if stat.stat_val is None else str(stat.stat_val)),
                        **dict([(k, v) for k, v in vars(stat).items() if k != 'stat_val']))
                    cls._create(EdgeTypes.Stat, entities, existing, created=created_at, **{
                        MagicProperties.FROM.value.name: column_vertex[MagicProperties.ID.value.name],
                        MagicProperties.TO.value.name: vertex[MagicProperties.ID.value.name]})

    @classmethod
    def user_entities(cls, *, user_data: List[User], entities: ENTITIES, existing: EXISTING,
                      created_at: datetime.datetime) -> None:
        for user in user_data:
            # TODO: handle this properly
            cls._create(VertexTypes.User, entities, existing, key=user.user_id,
                        **dict([(k, v) for k, v in vars(user).items() if k != 'other_key_values']))

    @classmethod
    def app_entities(cls, *, app_data: List[Application], entities: ENTITIES, existing: EXISTING,
                     created_at: datetime.datetime) -> None:
        for app in app_data:
            cls._create(VertexTypes.Application, entities, existing, key=app.id,
                        **dict((k, v) for k, v in vars(app).items()))

    @classmethod
    def _expire_other_edges(
            cls, *, edge_type: Union[EdgeTypes, EdgeType], vertex_id: str, to_or_from_vertex: MagicProperties,
            entities: ENTITIES, existing: EXISTING, created_at: datetime.datetime) -> None:
        """
        Use this in lieu of expire_previously_existing.

        :param edge_type:
        :param vertex_id:
        :param to_or_from_vertex:
        :param entities:
        :param existing:
        :param created_at:
        :return:
        """
        assert to_or_from_vertex in (MagicProperties.FROM, MagicProperties.TO), \
            f'only FROM or TO allowed for {to_or_from_vertex}'
        edge_type = ensure_edge_type(edge_type)
        # edges of that type....
        edges = tuple(e for e in existing.get(edge_type, {}).values()
                      # to/from the vertex
                      if e[to_or_from_vertex.value.name] == vertex_id
                      # edges that aren't recreated
                      and e[MagicProperties.ID.value.name] not in entities.get(edge_type, {}))
        # expire those:
        for entity in edges:
            del entities[edge_type][entity[MagicProperties.ID.value.name]]


class GetGraph:
    def __init__(self, *, g: GraphTraversalSource, created_at: Optional[datetime.datetime] = None) -> None:
        self.g = g
        self.created_at = datetime.datetime.now() if created_at is None else created_at
        self.existing = new_existing()
        self.entities = new_entities()
        self._expire_previously_existing_callables: List[Callable[[], None]] = list()

    @staticmethod
    def table_metric(table: Table) -> int:
        return _GetGraph.table_metric(table)

    def add_table_entities(self, table_data: List[Table]) -> "GetGraph":
        _FetchExisting.table_entities(table_data=table_data, _g=self.g, existing=self.existing)
        _GetGraph.table_entities(
            table_data=table_data, entities=self.entities, existing=self.existing, created_at=self.created_at)
        self._expire_previously_existing_callables.append(self._expire_previously_existing_table_entities)
        return self

    def _expire_previously_existing_table_entities(self) -> None:
        _GetGraph.expire_previously_existing(
            edge_types=(EdgeTypes.Column, EdgeTypes.Generates, EdgeTypes.Owner),
            entities=self.entities, existing=self.existing)

    def add_user_entities(self, user_data: List[User]) -> "GetGraph":
        _GetGraph.user_entities(
            user_data=user_data, entities=self.entities, existing=self.existing, created_at=self.created_at)
        self._expire_previously_existing_callables.append(self._expire_previously_existing_user_entities)
        return self

    def _expire_previously_existing_user_entities(self) -> None:
        pass

    def add_app_entities(self, app_data: List[Application]) -> "GetGraph":
        _GetGraph.app_entities(
            app_data=app_data, entities=self.entities, existing=self.existing, created_at=self.created_at)
        self._expire_previously_existing_callables.append(self._expire_previously_existing_app_entities)
        return self

    def _expire_previously_existing_app_entities(self) -> None:
        pass

    def complete(self) -> ENTITIES:
        for c in self._expire_previously_existing_callables:
            c()
        entities = self.entities
        del self.entities
        del self.existing
        return entities

    @classmethod
    def default_created_at(cls, created_at: Optional[datetime.datetime]) -> datetime.datetime:
        return datetime.datetime.now() if created_at is None else created_at

    @classmethod
    def table_entities(cls, *, table_data: List[Table], g: GraphTraversalSource,
                       created_at: Optional[datetime.datetime] = None) -> ENTITIES:
        return GetGraph(g=g, created_at=created_at).add_table_entities(table_data).complete()

    @classmethod
    def user_entities(cls, *, user_data: List[User], g: GraphTraversalSource,
                      created_at: Optional[datetime.datetime] = None) -> ENTITIES:
        return GetGraph(g=g, created_at=created_at).add_user_entities(user_data).complete()

    @classmethod
    def app_entities(cls, *, app_data: List[Application], g: GraphTraversalSource,
                     created_at: Optional[datetime.datetime] = None) -> ENTITIES:
        return GetGraph(g=g, created_at=created_at).add_app_entities(app_data).complete()

    @classmethod
    def expire_connections_for_other(
            cls, *, vertex_type: Union[VertexTypes, VertexType], keys: Iterable[str], g: GraphTraversalSource,
            created_at: Optional[datetime.datetime] = None) -> ENTITIES:
        """
        There's no builder style for this since the expiration implementation is presumptive.
        """
        if created_at is None:
            created_at = datetime.datetime.now()
        assert created_at is not None
        if not isinstance(keys, frozenset):
            keys = frozenset(keys)
        assert isinstance(keys, frozenset)
        vertex_type = ensure_vertex_type(vertex_type)
        existing = new_existing()
        entities = new_entities()
        _FetchExisting.expire_connections_for_other(vertex_type=vertex_type, keys=keys, existing=existing, _g=g)
        _GetGraph.expire_previously_existing(edge_types=tuple(t for t in EdgeTypes), entities=entities,
                                             existing=existing)
        return entities
