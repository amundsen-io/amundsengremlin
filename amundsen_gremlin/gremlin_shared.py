# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

from typing import Optional, no_type_check, overload

from gremlin_python.process.graph_traversal import GraphTraversal
from gremlin_python.process.traversal import Bytecode, Traversal


@no_type_check
def rsubstringstartingwith(sub: str, s: str) -> Optional[str]:
    """
    >>> rsubstringstartingwith('://', 'database://foo')
    'foo'
    >>> rsubstringstartingwith('://', 'database://foo://bar')
    'bar'
    >>> rsubstringstartingwith('://', 'foo')
    None
    """
    try:
        return s[s.rindex(sub) + len(sub):]
    except ValueError:
        return None


def make_database_uri(*, database_name: str) -> str:
    return f'database://{database_name}'


def get_database_name_from_uri(*, database_uri: str) -> str:
    if not database_uri.startswith('database://'):
        raise RuntimeError(f'database_uri is malformed! {database_uri}')
    database_name = rsubstringstartingwith('://', database_uri)
    if database_name is None:
        raise RuntimeError(f'database_uri is malformed! {database_uri}')
    return database_name


@overload
def make_cluster_uri(*, database_uri: str, cluster_name: str) -> str: ...


@overload
def make_cluster_uri(*, database_name: str, cluster_name: str) -> str: ...


def make_cluster_uri(*, cluster_name: str, database_uri: Optional[str] = None,
                     database_name: Optional[str] = None) -> str:
    if database_name is None:
        assert database_uri is not None
        database_name = get_database_name_from_uri(database_uri=database_uri)
    assert database_name is not None
    return f'{database_name}://{cluster_name}'


@overload
def make_schema_uri(*, cluster_uri: str, schema_name: str) -> str: ...


@overload
def make_schema_uri(*, database_name: str, cluster_name: str, schema_name: str) -> str: ...


def make_schema_uri(*, schema_name: str, cluster_uri: Optional[str] = None, database_name: Optional[str] = None,
                    cluster_name: Optional[str] = None) -> str:
    if cluster_uri is None:
        assert cluster_name is not None and database_name is not None
        cluster_uri = make_cluster_uri(cluster_name=cluster_name, database_name=database_name)
    assert cluster_uri is not None
    return f'{cluster_uri}.{schema_name}'


@overload
def make_table_uri(*, schema_uri: str, table_name: str) -> str: ...


@overload
def make_table_uri(*, database_name: str, cluster_name: str, schema_name: str, table_name: str) -> str: ...


def make_table_uri(*, table_name: str, schema_uri: Optional[str] = None, database_name: Optional[str] = None,
                   cluster_name: Optional[str] = None, schema_name: Optional[str] = None) -> str:
    if schema_uri is None:
        assert database_name is not None and cluster_name is not None and schema_name is not None
        schema_uri = make_schema_uri(schema_name=schema_name, cluster_name=cluster_name, database_name=database_name)
    assert schema_uri is not None
    return f'{schema_uri}/{table_name}'


def make_message_uri(*, name: str, package: str) -> str:
    return f'message/{package}.{name}'


def make_shard_uri(*, table_uri: str, shard_name: str) -> str:
    return f'{table_uri}/shards/{shard_name}'


def make_description_uri(*, subject_uri: str, source: str) -> str:
    return f'{subject_uri}/{source}/_description'


def make_column_uri(*, table_uri: str, column_name: str) -> str:
    return f'{table_uri}/{column_name}'


def make_column_statistic_uri(*, column_uri: str, statistic_type: str) -> str:
    return f'{column_uri}/stat/{statistic_type}'


def append_traversal(g: Traversal, *traversals: Optional[Traversal]) -> GraphTraversal:
    """
    copy the traversal, and append the traversals to it.  (It's a little magic, but common-ish in the gremlin world
    forums.)
    """
    bytecode = Bytecode(bytecode=g.bytecode)
    for t in [t for t in traversals if t is not None]:
        assert t.graph is None, f'traversal has a graph source!  should be an anonymous traversal: {t}'
        for source_name, *source_args in t.bytecode.source_instructions:
            bytecode.add_source(source_name, *source_args)
        for step_name, *step_args in t.bytecode.step_instructions:
            bytecode.add_step(step_name, *step_args)
    return GraphTraversal(graph=g.graph, traversal_strategies=g.traversal_strategies, bytecode=bytecode)
