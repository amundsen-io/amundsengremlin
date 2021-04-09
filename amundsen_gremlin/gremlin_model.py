# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import datetime
from abc import ABC, abstractmethod
from enum import Enum, unique
from functools import lru_cache
from typing import (
    Any, Dict, FrozenSet, Hashable, List, Mapping, NamedTuple, Optional,
    Sequence, Set, Tuple, Type, TypeVar
)

from gremlin_python.process.traversal import Cardinality
from overrides import overrides

from amundsen_gremlin.test_and_development_shard import get_shard


class GremlinTyper(ABC):
    @abstractmethod
    def is_allowed(self, value: Any) -> None:
        pass

    def format(self, value: Any) -> str:
        self.is_allowed(value)
        # default is to use the built-in format
        return str(value)


class GremlinBooleanTyper(GremlinTyper):
    def is_allowed(self, value: Any) -> None:
        assert isinstance(value, bool), f'expected bool, not {type(value)} {value}'


class GremlinByteTyper(GremlinTyper):
    def is_allowed(self, value: Any) -> None:
        assert isinstance(value, int) and value >= -(2**7) and value < (2**7), \
            f'expected int in [-2**7, 2**7), not {type(value)} {value}'


class GremlinShortTyper(GremlinTyper):
    def is_allowed(self, value: Any) -> None:
        assert isinstance(value, int) and value >= -(2**15) and value < (2**15), \
            f'expected int in [-2**15, 2**15), not {type(value)} {value}'


class GremlinIntTyper(GremlinTyper):
    def is_allowed(self, value: Any) -> None:
        assert isinstance(value, int) and value >= -(2**31) and value < (2**31), \
            f'expected int in [-2**31, 2**31), not {type(value)} {value}'


class GremlinLongTyper(GremlinTyper):
    def is_allowed(self, value: Any) -> None:
        assert isinstance(value, int) and value >= -(2**63) and value < (2**63), \
            f'expected int in [-2**63, 2**63), not {type(value)} {value}'


class GremlinFloatTyper(GremlinTyper):
    """
    The Neptune Bulk Loader loads any precision floating point and rounds the mantissa.  It'd be nice to avoid the
    possible surprise by checking the precision here but feels too strict.
    """
    def is_allowed(self, value: Any) -> None:
        assert isinstance(value, float), f'expected float, not {type(value)} {value}'


class GremlinStringTyper(GremlinTyper):
    def is_allowed(self, value: Any) -> None:
        assert isinstance(value, str), f'expected str, not {type(value)} {value}'


class GremlinDateTyper(GremlinTyper):
    @overrides
    def is_allowed(self, value: Any) -> None:
        assert (isinstance(value, (datetime.datetime, datetime.date))
                and (value.tzinfo is None if isinstance(value, (datetime.datetime)) else True)), \
            f'expected datetime.datetime (without tz) or datetime.date, not {type(value)} {value}'

    @overrides
    def format(self, value: Any) -> str:
        # datetime.datetime first otherwise isinstance(datetime.date) will catch it
        if isinstance(value, datetime.datetime):
            # already asserted no tz but double check
            assert value.tzinfo is None, f'wat? already checked there was no tzinfo {value}'
            return value.isoformat(timespec='seconds')
        elif isinstance(value, datetime.date):
            return value.isoformat()
        else:
            raise AssertionError(f'wat? already checked value was datetime or date: {value}')


class GremlinType(Enum):
    """
    https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html
    Note I really wish the enum *values* had types too, but Guido didn't seem to like that at all.
    """
    # 'Bool', 'Boolean'
    Boolean = GremlinBooleanTyper()
    # 'Byte', 'Short', 'Int', 'Long'
    Byte = GremlinByteTyper()
    Short = GremlinShortTyper()
    Int = GremlinIntTyper()
    Long = GremlinLongTyper()
    # 'Float', 'Double': the loader loads any precision floating point and rounds the mantissa
    Float = GremlinFloatTyper()
    Double = GremlinFloatTyper()
    # 'String'
    String = GremlinStringTyper()
    # 'Date' in YYYY-MM-DD, YYYY-MM-DDTHH:mm, YYYY-MM-DDTHH:mm:SS, YYYY-MM-DDTHH:mm:SSZ
    Date = GremlinDateTyper()


class GremlinCardinality(Enum):
    single = Cardinality.single
    set = Cardinality.set_
    # list is not supported by Neptune
    list = Cardinality.list_

    def gremlin_python_cardinality(self) -> Cardinality:
        return self.value


class Property(NamedTuple):
    name: str
    type: GremlinType
    # For edge properties, omit (always single).  For vertex properties, we assume single (unlike Neptune's gremlin
    # which would assume set).
    cardinality: Optional[GremlinCardinality] = None
    multi_valued: bool = False
    required: bool = False
    comment: Optional[str] = None
    default: Optional[Any] = None

    def signature(self, default_cardinality: Optional[GremlinCardinality]) -> 'Property':
        # this isn't foolproof but works for Property and MagicProperty at least
        return type(self)(name=self.name, type=self.type, cardinality=self.cardinality or default_cardinality)

    def format(self, value: Any) -> str:
        return self.type.value.format(value)

    def header(self) -> str:
        formatted = f'{self.name}:{self.type.name}'
        if self.cardinality:
            formatted += f'({self.cardinality.name})'
        if self.multi_valued:
            formatted += '[]'
        return formatted


class MagicProperty(Property):
    @overrides
    def header(self) -> str:
        return self.name


@unique
class MagicProperties(Enum):
    """
    when writing out the header for these, they don't get the cardinality (they're single) or multi-valued
    (they're single)
    """
    ID = MagicProperty(name='~id', type=GremlinType.String, required=True)
    LABEL = MagicProperty(name='~label', type=GremlinType.String, required=True)
    FROM = MagicProperty(name='~from', type=GremlinType.String, required=True)
    TO = MagicProperty(name='~to', type=GremlinType.String, required=True)


@unique
class WellKnownProperties(Enum):
    # we expect that key is unique for a label
    Key = Property(name='key', type=GremlinType.String, required=True)

    Created = Property(name='created', type=GremlinType.Date, required=True)
    Expired = Property(name='expired', type=GremlinType.Date)

    TestShard = Property(name='shard', type=GremlinType.String, required=True, comment='''
    Only present in development and testing.  Separates different instances sharing a datastore (so is sort of the
    opposite of how one might usually use the word shard).
    ''')


# TODO: move this someplace shared
def _discover_parameters(format_string: str) -> FrozenSet[str]:
    """
    use this to discover what the parameters are to a format string (e.g. what parameters we need for a vertex id)
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


V = TypeVar('V')


class VertexTypeIdFormats(Enum):
    # note, these aren't f-strings, but their formatted values are used
    DEFAULT = '{~label}:{key}'


class VertexType(NamedTuple):
    label: str
    properties: Tuple[Property, ...]
    id_format: str = 'not used'
    defaults: Tuple[Tuple[str, Hashable], ...] = ()

    # let's make this simpler and say no positional args (which the NamedTuple constructor would allow), also we can't
    # override __new__
    @classmethod
    def construct_type(cls: Type["VertexType"], **kwargs: Any) -> "VertexType":
        defaults: Dict[str, Hashable] = dict()
        properties: Set[Property] = set(kwargs.pop('properties', []))
        properties.update({MagicProperties.LABEL.value, MagicProperties.ID.value, WellKnownProperties.Key.value})

        # (magically) insinuate the shard identifier into the vertex id format
        shard = get_shard()
        id_format = kwargs.pop('id_format', VertexTypeIdFormats.DEFAULT.value)
        if shard:
            properties.update({WellKnownProperties.TestShard.value})
            defaults.update({WellKnownProperties.TestShard.value.name: shard})
            # prepend if it's not in the format already (which would be pretty weird, but anyway)
            if '{shard}' not in id_format:
                id_format = '{shard}:' + id_format

        parameters = _discover_parameters(id_format)
        properties_names = set([p.name for p in properties])
        assert all(p in properties_names for p in parameters), \
            f'id_format: {id_format} has parameters: {parameters} not found in our properties {properties_names}'

        return cls(properties=tuple(properties), defaults=tuple(defaults.items()), id_format=id_format, **kwargs)

    @lru_cache()
    def properties_as_map(self) -> Mapping[str, Property]:
        mapping = dict([(p.name, p) for p in self.properties])
        assert len(mapping) == len(self.properties), f'are property names not unique? {self.properties}'
        return mapping

    def id(self, **entity: Any) -> str:
        for name, value in (self.defaults or ()):
            if name not in entity:
                entity[name] = value
        # format them if they're not already.  (the isinstance(v, str) feels wrong here tho)
        values = dict([(n, (self.properties_as_map()[n].format(v) if v is not None and not isinstance(v, str) else v))
                       for n, v in entity.items()])
        values.update({'~label': self.label})
        return self.id_format.format(**values)

    def create(self, **properties: Any) -> Mapping[str, Any]:
        if MagicProperties.ID.value in self.properties and MagicProperties.ID.value.name not in properties:
            properties[MagicProperties.ID.value.name] = self.id(**properties)
        if MagicProperties.LABEL.value in self.properties and MagicProperties.LABEL.value.name not in properties:
            properties[MagicProperties.LABEL.value.name] = self.label
        for name, value in (self.defaults or ()):
            if name not in properties:
                properties[name] = value
        # remove missing values
        for k in [k for k, v in properties.items() if v is None]:
            del properties[k]
        properties.update([(k, v) for k, v in self.properties_as_map().items()
                           if v.default is not None and k not in properties])
        property_names = set(self.properties_as_map().keys())
        assert set(properties.keys()).issubset(property_names), \
            f'unexpected properties: properties: {properties}, expected names: {property_names}'
        required_property_names = set([k for k, v in self.properties_as_map().items() if v.required])
        assert set(properties.keys()).issuperset(required_property_names), \
            f'expected required properties: properties: {properties}, expected names: {required_property_names}'
        return properties


class EdgeTypeIdFormats(Enum):
    # note, these aren't f-strings, but their formatted values are used
    DEFAULT = '{~label}:{~from}->{~to}'
    EXPIRABLE = '{~label}:{created}:{~from}->{~to}'


class EdgeType(NamedTuple):
    label: str
    properties: Tuple[Property, ...]
    # TODO: fill these out
    from_labels: Tuple[str, ...] = ()
    to_labels: Tuple[str, ...] = ()
    id_format: str = 'not used'

    # let's make this simpler and say no positional args (which the NamedTuple constructor would allow), also we can't
    # override __new__
    @classmethod
    def construct_type(cls: Type["EdgeType"], *, expirable: bool = True, **kwargs: Any) -> "EdgeType":
        properties: Set[Property] = set(kwargs.pop('properties', []))
        properties.update({MagicProperties.LABEL.value, MagicProperties.ID.value, MagicProperties.FROM.value,
                           MagicProperties.TO.value, WellKnownProperties.Created.value})

        # NB: Some edge types may not make sense to soft-expire
        if expirable:
            properties.update({WellKnownProperties.Expired.value})
            id_format = kwargs.pop('id_format', EdgeTypeIdFormats.EXPIRABLE.value)
        else:
            id_format = kwargs.pop('id_format', EdgeTypeIdFormats.DEFAULT.value)

        parameters = _discover_parameters(id_format)
        properties_names = set([p.name for p in properties])
        assert all(p in properties_names for p in parameters), \
            f'id_format: {id_format} has parameters: {parameters} not found in our properties {properties_names}'

        return cls(properties=tuple(properties), id_format=id_format, **kwargs)

    @lru_cache()
    def properties_as_map(self) -> Mapping[str, Property]:
        mapping = dict([(p.name, p) for p in self.properties])
        assert len(mapping) == len(self.properties), f'are property names not unique? {self.properties}'
        return mapping

    def id(self, **entity: Any) -> str:
        # format them if they're not already.  (the isinstance(v, str) feels wrong here tho)
        values = dict([(n, (self.properties_as_map()[n].format(v) if v is not None and not isinstance(v, str) else v))
                       for n, v in entity.items()])
        values.update({'~label': self.label})
        return self.id_format.format(**values)

    def create(self, **properties: Any) -> Mapping[str, Any]:
        properties = dict(properties)
        if MagicProperties.ID.value in self.properties and MagicProperties.ID.value.name not in properties:
            properties[MagicProperties.ID.value.name] = self.id(**properties)
        if MagicProperties.LABEL.value in self.properties and MagicProperties.LABEL.value.name not in properties:
            properties[MagicProperties.LABEL.value.name] = self.label
        # remove missing values
        for k in [k for k, v in properties.items() if v is None]:
            del properties[k]
        properties.update([(k, v) for k, v in self.properties_as_map().items()
                           if v.default is not None and k not in properties])
        property_names = set(self.properties_as_map().keys())
        assert set(properties.keys()).issubset(property_names), \
            f'unexpected properties: properties: {properties}, expected names: {property_names}'
        required_property_names = set([k for k, v in self.properties_as_map().items() if v.required])
        assert set(properties.keys()).issuperset(required_property_names), \
            f'expected required properties: properties: {properties}, expected names: {required_property_names}'
        return properties


class VertexTypes(Enum):
    """
    In general, you will need to reload all your data if you: 1. change label, 2. if you change the type of a property,
    3. change the effective id_format
    """
    @classmethod
    @lru_cache()
    def by_label(cls) -> Mapping[str, "VertexTypes"]:
        constants: List[VertexTypes] = list(cls)
        mapping = dict([(c.value.label, c) for c in constants])
        assert len(mapping) == len(constants), f'are label names not unique? {constants}'
        return mapping

    Application = VertexType.construct_type(
        label='Application',
        properties=[
            # there's a kind property we don't care about so much, so are ignoring and assuming all Application with
            # the same id (but different kind) have the same identity
            Property(name='id', type=GremlinType.String, required=True),
            Property(name='name', type=GremlinType.String),
            Property(name='description', type=GremlinType.String),
            # except: we get different application_url per kind so keep those (but if a kind's url changes, we'll keep
            # the old one around so the model isn't perfect)
            Property(name='application_url', type=GremlinType.String, cardinality=GremlinCardinality.set)])
    Column = VertexType.construct_type(
        label='Column',
        properties=[
            Property(name='name', type=GremlinType.String, required=True),
            Property(name='sort_order', type=GremlinType.Int),
            Property(name='col_type', type=GremlinType.String)])
    Cluster = VertexType.construct_type(
        label='Cluster',
        properties=[Property(name='name', type=GremlinType.String, required=True)])
    Database = VertexType.construct_type(
        label='Database',
        properties=[
            Property(name='name', type=GremlinType.String, required=True)])
    Description = VertexType.construct_type(
        label='Description',
        properties=[
            Property(name='description', type=GremlinType.String, required=True),
            Property(name='description_source', type=GremlinType.String, required=True, comment='effectively an enum')])
    Programmatic_Description = VertexType.construct_type(
        label='Programmatic_Description',
        properties=[
            Property(name='description', type=GremlinType.String, required=True),
            Property(name='description_source', type=GremlinType.String, required=True, comment='effectively an enum')])
    Schema = VertexType.construct_type(
        label='Schema',
        properties=[Property(name='name', type=GremlinType.String, required=True)])
    Source = VertexType.construct_type(
        label='Source',
        properties=[])
    Stat = VertexType.construct_type(
        label='Stat',
        properties=[
            Property(name='stat_val', type=GremlinType.String),
            Property(name='stat_type', type=GremlinType.String, comment='effectively an enum'),
            Property(name='start_epoch', type=GremlinType.Date),
            Property(name='end_epoch', type=GremlinType.Date)])
    Table = VertexType.construct_type(
        label='Table',
        properties=[
            Property(name='name', type=GremlinType.String, required=True),
            Property(name='is_view', type=GremlinType.Boolean),
            Property(name='display_name', type=GremlinType.String)])
    Tag = VertexType.construct_type(
        label='Tag',
        properties=[
            Property(name='tag_name', type=GremlinType.String, required=True),
            Property(name='tag_type', type=GremlinType.String, required=True, default='default',
                     comment='effectively an enum, usually default')])
    Timestamp = VertexType.construct_type(
        label='Timestamp',
        properties=[])
    Updatedtimestamp = VertexType.construct_type(
        label='Updatedtimestamp',
        properties=[
            Property(name='latest_timestamp', type=GremlinType.Date, required=True)])
    User = VertexType.construct_type(
        label='User',
        properties=[
            Property(name='user_id', type=GremlinType.String, required=True),
            Property(name='email', type=GremlinType.String),
            Property(name='full_name', type=GremlinType.String),
            Property(name='first_name', type=GremlinType.String),
            Property(name='last_name', type=GremlinType.String),
            Property(name='display_name', type=GremlinType.String),
            Property(name='team_name', type=GremlinType.String),
            Property(name='employee_type', type=GremlinType.String, comment='this is effectively an enum'),
            Property(name='is_active', type=GremlinType.Boolean),
            Property(name='profile_url', type=GremlinType.String),
            Property(name='role_name', type=GremlinType.String),
            Property(name='slack_id', type=GremlinType.String),
            Property(name='github_username', type=GremlinType.String),
            Property(name='manager_fullname', type=GremlinType.String),
            Property(name='manager_email', type=GremlinType.String),
            Property(name='manager_id', type=GremlinType.String,
                     comment='the key/user_id of another User who is the manager for this User'),
            Property(name='is_robot', type=GremlinType.Boolean)
        ])
    Watermark = VertexType.construct_type(
        label='Watermark',
        properties=[])


class EdgeTypes(Enum):
    """
    In general, you will need to reload all your data if you: 1. change label, 2. if you change the type of a property,
    3. change the effective id_format (e.g. change required)
    """
    @classmethod
    @lru_cache()
    def by_label(cls) -> Mapping[str, "EdgeTypes"]:
        constants: List[EdgeTypes] = list(cls)
        mapping = dict([(c.value.label, c) for c in constants])
        assert len(mapping) == len(constants), f'are label names not unique? {constants}'
        return mapping

    @classmethod
    @lru_cache()
    def expirable(cls: Type["EdgeTypes"]) -> Sequence["EdgeTypes"]:
        return tuple(t for t in cls
                     if WellKnownProperties.Expired.value.name in t.value.properties_as_map())

    Admin = EdgeType.construct_type(label='ADMIN')
    BelongToTable = EdgeType.construct_type(label='BELONG_TO_TABLE')
    Cluster = EdgeType.construct_type(label='CLUSTER')
    Column = EdgeType.construct_type(label='COLUMN')
    Database = EdgeType.construct_type(label='DATABASE')
    Description = EdgeType.construct_type(label='DESCRIPTION')
    Follow = EdgeType.construct_type(label='FOLLOW')
    Generates = EdgeType.construct_type(label='GENERATES')
    LastUpdatedAt = EdgeType.construct_type(label='LAST_UPDATED_AT')
    ManagedBy = EdgeType.construct_type(label='MANAGED_BY')
    Owner = EdgeType.construct_type(label='OWNER')
    Read = EdgeType.construct_type(
        label='READ',
        # need to do something safer with that date (so it doesn't end up as datetime ever)
        id_format='{~label}:{date}:{~from}->{~to}',
        properties=[
            Property(name='date', type=GremlinType.Date, required=True),
            Property(name='read_count', type=GremlinType.Long, required=True)])
    ReadWrite = EdgeType.construct_type(label='READ_WRITE')
    ReadOnly = EdgeType.construct_type(label='READ_ONLY')
    Schema = EdgeType.construct_type(label='SCHEMA')
    Source = EdgeType.construct_type(label='SOURCE')
    Stat = EdgeType.construct_type(label='STAT')
    Table = EdgeType.construct_type(label='TABLE')
    Tag = EdgeType.construct_type(label='TAG')
