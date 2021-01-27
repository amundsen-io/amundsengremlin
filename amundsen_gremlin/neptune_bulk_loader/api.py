# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import csv
import datetime
import json
import logging
import time
from collections import defaultdict
from enum import Enum, auto
from io import BytesIO, StringIO
from typing import (
    IO, Any, Callable, Collection, Dict, Iterable, List, Mapping, Optional,
    Sequence, Set, Tuple, Type, TypeVar, Union, cast
)
from urllib.parse import SplitResult, urlencode, urlsplit, urlunsplit

import boto3
import requests
from boto3.s3.transfer import TransferConfig
from flask import Config
from gremlin_python.driver.driver_remote_connection import (
    DriverRemoteConnection
)
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import GraphTraversalSource
from neptune_python_utils.endpoints import Endpoints, RequestParameters
from requests_aws4auth import AWS4Auth
from tornado import httpclient
from typing_extensions import TypedDict  # is in typing in 3.8

from amundsen_gremlin.gremlin_model import (
    EdgeType, GremlinCardinality, Property, VertexType
)
from amundsen_gremlin.test_and_development_shard import get_shard
from for_requests.assume_role_aws4auth import AssumeRoleAWS4Auth
from for_requests.aws4auth_compatible import to_aws4_request_compatible_host
from for_requests.host_header_ssl import HostHeaderSSLAdapter
from ssl_override_server_hostname.ssl_context import (
    OverrideServerHostnameSSLContext
)

LOGGER = logging.getLogger(__name__)

GraphEntity = Mapping[str, Any]
FormattedEntity = Mapping[str, str]
GraphEntityType = Union[VertexType, EdgeType]
PropertiesMap = Mapping[str, Property]
GraphEntities = Mapping[GraphEntityType, List[GraphEntity]]
FormattedEntities = Sequence[Mapping[str, str]]


def get_neptune_graph_traversal_source_factory_from_config(config: Config) -> Callable[[], GraphTraversalSource]:
    session = config.get('NEPTUNE_SESSION')
    assert session is not None

    neptune_url = config.get('NEPTUNE_URL')
    assert neptune_url is not None

    return get_neptune_graph_traversal_source_factory(neptune_url=neptune_url, session=session)


def get_neptune_graph_traversal_source_factory(*, neptune_url: Union[str, Mapping[str, Any]],
                                               session: boto3.session.Session) -> Callable[[], GraphTraversalSource]:

    endpoints: Endpoints
    override_uri: Optional[str]
    if isinstance(neptune_url, str):
        uri = urlsplit(neptune_url)
        assert uri.scheme in ('wss', 'ws') and uri.path == '/gremlin' and not uri.query and not uri.fragment, \
            f'expected Neptune URL not {neptune_url}'
        endpoints = Endpoints(neptune_endpoint=uri.hostname, neptune_port=uri.port,
                              region_name=session.region_name, credentials=session.get_credentials())
        override_uri = None
    elif isinstance(neptune_url, Mapping):
        endpoints = Endpoints(neptune_endpoint=neptune_url['neptune_endpoint'],
                              neptune_port=neptune_url['neptune_port'], region_name=session.region_name,
                              credentials=session.get_credentials())
        override_uri = neptune_url['uri']
        assert override_uri is None or isinstance(override_uri, str)
    else:
        raise AssertionError(f'what is NEPTUNE_URL? {neptune_url}')

    def create_graph_traversal_source(**kwargs: Any) -> GraphTraversalSource:
        assert all(e not in kwargs for e in ('url', 'traversal_source')), \
            f'do not pass traversal_source or url in {kwargs}'
        prepared_request = override_prepared_request_parameters(
            endpoints.gremlin_endpoint().prepare_request(), override_uri=override_uri)
        kwargs['traversal_source'] = 'g'
        remote_connection = DriverRemoteConnection(url=prepared_request, **kwargs)
        return traversal().withRemote(remote_connection)
    return create_graph_traversal_source


def override_prepared_request_parameters(
        request_parameters: RequestParameters, *, override_uri: Optional[Union[str, SplitResult]] = None,
        method: Optional[str] = None, data: Optional[str] = None) -> httpclient.HTTPRequest:
    """
    use like:
    endpoints = Endpoints(neptune_endpoint=host_name, neptune_port=port_number,
                          region_name=session.region_name, credentials=session.get_credentials())
    override_prepared_request(endpoints.gremlin_endpoint().prepare_request(), override_uri=host_to_actually_connect_to)

    but note if you are not GETing (or have a payload), prepare_request doesn't *actually* generate sufficient headers
    (despite the fact that it accepts a method)
    """
    http_request_param: Dict[str, Any] = dict(url=request_parameters.uri, headers=request_parameters.headers)
    if method is not None:
        http_request_param['method'] = method
    if data is not None:
        http_request_param['body'] = data
    if override_uri is not None:
        # we override the URI slightly (because the instance thinks it's a different host than we're connecting to)
        if isinstance(override_uri, str):
            override_uri = urlsplit(override_uri)
        assert isinstance(override_uri, SplitResult)
        uri = urlsplit(request_parameters.uri)
        http_request_param['headers'] = dict(request_parameters.headers)
        http_request_param['headers']['Host'] = uri.netloc
        http_request_param['ssl_options'] = OverrideServerHostnameSSLContext(server_hostname=uri.hostname)
        http_request_param['url'] = urlunsplit(
            (uri.scheme, override_uri.netloc, uri.path, uri.query, uri.fragment))
    return httpclient.HTTPRequest(**http_request_param)


def _urlsplit_if_not_already(uri: Union[str, SplitResult]) -> SplitResult:
    if isinstance(uri, str):
        return urlsplit(uri)
    elif isinstance(uri, SplitResult):
        return uri
    raise AssertionError(f'what is uri? {uri}')


def request_with_override(*, uri: Union[str, SplitResult], override_uri: Optional[Union[str, SplitResult]] = None,
                          method: str = 'GET', headers: Dict[str, str] = {}, **kwargs: Any) -> Any:
    # why not use endpoints? Despite the fact that it accepts a method and payload, it doesn't *actually* generate
    # sufficient headers so we'll use requests for these since we can
    if isinstance(uri, str):
        uri = urlsplit(uri)
    elif isinstance(uri, SplitResult):
        pass
    else:
        raise AssertionError(f'what is uri? {uri}')

    # don't always need this, but it doesn't hurt
    if 'Host' not in headers:
        headers = dict(headers)
        headers['Host'] = to_aws4_request_compatible_host(uri)
    s = requests.Session()
    if override_uri:
        override_uri = _urlsplit_if_not_already(override_uri)
        uri = urlunsplit((uri.scheme, override_uri.netloc, uri.path, uri.query, uri.fragment))
        s.mount('https://', HostHeaderSSLAdapter())
    else:
        uri = urlunsplit(uri)
    return s.request(method=method, url=uri, headers=headers, **kwargs)


def response_as_json(response: Any) -> Mapping[str, Any]:
    return json.loads(response.content.decode('utf-8'))


class NeptuneBulkLoaderLoadStatusOverallStatus(TypedDict):
    fullUri: str
    runNumber: int
    retryNumber: int
    status: str  # Literal['LOAD_FAILED', 'LOAD_COMPLETED', ...]
    totalTimeSpent: int
    startTime: int
    totalRecords: int
    totalDuplicates: int
    parsingErrors: int
    datatypeMismatchErrors: int
    insertErrors: int


class NeptuneBulkLoaderLoadStatusErrorLogEntry(TypedDict):
    errorCode: str
    errorMessage: str
    fileName: str
    recordNum: int


class NeptuneBulkLoaderLoadStatusErrors(TypedDict):
    startIndex: int
    endIndex: int
    loadId: str
    # depends on the errors_per_page and errors_page
    errorLogs: List[NeptuneBulkLoaderLoadStatusErrorLogEntry]


class NeptuneBulkLoaderLoadStatusPayload(TypedDict):
    # those string keys are like the status enums
    feedCount: List[Dict[str, int]]
    overallStatus: NeptuneBulkLoaderLoadStatusOverallStatus
    # optional, only if errors is true in the request
    errors: NeptuneBulkLoaderLoadStatusErrors


class NeptuneBulkLoaderLoadStatus(TypedDict):
    """
    see https://docs.aws.amazon.com/neptune/latest/userguide/load-api-reference-status.html
    """
    status: str
    payload: NeptuneBulkLoaderLoadStatusPayload


class BulkLoaderParallelism(Enum):
    # TODO: Literal might be better for this in 3.8?
    LOW = auto()
    MEDIUM = auto()
    HIGH = auto()
    OVERSUBSCRIBE = auto()


class BulkLoaderFormat(Enum):
    """
    See https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format.html
    """
    # see https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-gremlin.html
    CSV = 'csv'
    # see https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-format-rdf.html
    N_TRIPLES = 'ntriples'
    N_QUADS = 'nquads'
    RDF_XML = 'rdfxml'
    TURTLE = 'turtle'


class NeptuneBulkLoaderApi:
    def __init__(self, *, session: boto3.session.Session, endpoint_uri: Union[str, SplitResult],
                 override_uri: Optional[Union[str, SplitResult]] = None,
                 iam_role_name: str = 'NeptuneLoadFromS3', s3_bucket_name: str) -> None:
        self.session = session
        self.endpoint_uri = _urlsplit_if_not_already(endpoint_uri)
        assert self.endpoint_uri.path == '/gremlin' and self.endpoint_uri.scheme in ('ws', 'wss') and \
            not self.endpoint_uri.query, f'expected gremlin uri: {endpoint_uri}'
        self.override_uri = _urlsplit_if_not_already(override_uri) if override_uri is not None else None
        account_id = self.session.client('sts').get_caller_identity()['Account']
        self.iam_role_arn = f'arn:aws:iam::{account_id}:role/{iam_role_name}'
        self.s3_bucket_name = s3_bucket_name
        # See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3.html#using-the-transfer-manager
        self.s3_transfer_config = TransferConfig(
            multipart_threshold=100 * (2 ** 20),  # 100MB
            max_concurrency=5)

    @classmethod
    def create_from_config(cls: Type["NeptuneBulkLoaderApi"], config: Mapping[Any, Any]) -> "NeptuneBulkLoaderApi":
        neptune_url = config.get('NEPTUNE_URL')
        endpoint_uri: Optional[str]
        override_uri: Optional[str]
        if isinstance(neptune_url, str):
            endpoint_uri = neptune_url
            override_uri = None
        elif isinstance(neptune_url, Mapping):
            endpoint_uri = f"wss://{neptune_url['neptune_endpoint']}:{neptune_url['neptune_port']}/gremlin"
            override_uri = neptune_url['uri']
        else:
            raise AssertionError(f'expected NEPTUNE_URL to be a str or dict: {neptune_url}')
        s3_bucket_name = config.get('NEPTUNE_BULK_LOADER_S3_BUCKET_NAME')
        assert s3_bucket_name is not None and isinstance(s3_bucket_name, str)
        return cls(endpoint_uri=endpoint_uri, override_uri=override_uri, session=config.get('NEPTUNE_SESSION'),
                   s3_bucket_name=s3_bucket_name)

    def _get_aws4auth(self, service_name: str) -> AWS4Auth:
        return AssumeRoleAWS4Auth(self.session.get_credentials(), self.session.region_name, service_name)

    def upload(self, *, f: IO[bytes], s3_object_key: str) -> None:
        """
        e.g.

        with BytesIO(vertex_csv) as f:
            api.upload(f=f, s3_object_key=f'{object_prefix}/vertex.csv')
        """
        s3_client = self.session.client('s3')
        return s3_client.upload_fileobj(f, self.s3_bucket_name, s3_object_key, Config=self.s3_transfer_config)

    def load(self, *, s3_object_key: str, dependencies: List[str] = [],
             parallelism: BulkLoaderParallelism = BulkLoaderParallelism.HIGH, failOnError: bool = False,
             updateSingleCardinalityProperties: bool = True, queueRequest: bool = True, **kwargs: Any) -> Any:
        uri = urlunsplit(('https' if self.endpoint_uri.scheme == 'wss' else 'http', self.endpoint_uri.netloc, 'loader',
                          self.endpoint_uri.query, self.endpoint_uri.fragment))
        response = request_with_override(
            method='POST', uri=uri, override_uri=self.override_uri, auth=self._get_aws4auth('neptune-db'),
            proxies=dict(http=None, https=None),
            data=dict(source=f's3://{self.s3_bucket_name}/{s3_object_key}',
                      format=BulkLoaderFormat.CSV.value,
                      iamRoleArn=self.iam_role_arn,
                      region=self.session.region_name,
                      failOnError=failOnError,
                      parallelism=parallelism.name,
                      updateSingleCardinalityProperties=updateSingleCardinalityProperties,
                      queueRequest=queueRequest,
                      dependencies=dependencies),
            **kwargs)
        return response_as_json(response)

    def load_status(self, *, load_id: str = '', errors: bool = False, errors_per_page: int = 10, errors_page: int = 1,
                    **kwargs: Any) -> NeptuneBulkLoaderLoadStatus:
        """
        See https://docs.aws.amazon.com/neptune/latest/userguide/load-api-reference-status.html
        """
        query_parameters = dict()
        if errors:
            query_parameters.update(dict(errors=errors, errorsPerPage=errors_per_page, page=errors_page))
        uri = urlunsplit(('https' if self.endpoint_uri.scheme == 'wss' else 'http', self.endpoint_uri.netloc,
                          f'loader/{load_id}', urlencode(query_parameters), self.endpoint_uri.fragment))
        response = request_with_override(uri=uri, override_uri=self.override_uri, auth=self._get_aws4auth('neptune-db'),
                                         proxies=dict(http=None, https=None))
        return cast(NeptuneBulkLoaderLoadStatus, response_as_json(response))

    def bulk_load_entities(
            self, *, entities: Mapping[GraphEntityType, Mapping[str, GraphEntity]], object_prefix: Optional[str] = None,
            polling_period: int = 10, raise_if_failed: bool = False) -> Mapping[str, Mapping[str, Any]]:
        """
        :param entities:  The entities being bulk loaded.  They will be partitioned at least by vertex vs edge, but
        possibly by conflicting property type (though the latter is unusual), and written to files in S3, then loaded
        by Neptune.
        :param object_prefix: (optional)   The string is treated like a format string, and 'now' and 'shard' (if
        get_shard() is truthy) are the available parameters. Defaults to '{now}/{shard}' or '{now}'.
        :param polling_period: (optional) defaults to 10 (seconds).  The period at which the status will be polled.
        :param raise_if_failed: (optional) defaults to False.  If True, will raise if any of the loads failed, otherwise
        log a warning and return the status.  True would be useful for testing or other situations where you would
        always expect the load to succeed.
        :return:
        """
        format_args = dict(
            now=datetime.datetime.now().isoformat(timespec='milliseconds').replace(':', '-').replace('.', '-'))
        shard = get_shard()
        if shard:
            format_args.update(shard=shard)
        if not object_prefix:
            object_prefix = '{now}/{shard}' if 'shard' in format_args else '{now}'
        object_prefix = object_prefix.format(**format_args)

        assert isinstance(object_prefix, str) and all(c not in object_prefix for c in ':'), \
            f'object_prefix is going to break S3 {object_prefix}'

        vertexes, edges = group_by_class(entities)

        # TODO: write these to tmp? stream them in?
        vertex_csvs: List[bytes] = []
        for types in partition_properties(vertexes.keys()):
            with StringIO() as w:
                write_entities_as_csv(w, dict((t, vertexes[t]) for t in types))
                vertex_csvs.append(w.getvalue().encode('utf-8'))

        edge_csvs: List[bytes] = []
        for types in partition_properties(edges.keys()):
            with StringIO() as w:
                write_entities_as_csv(w, dict((t, edges[t]) for t in types))
                edge_csvs.append(w.getvalue().encode('utf-8'))

        csvs: List[Tuple[str, bytes]] = [(f'{object_prefix}/vertex{i}.csv', v) for i, v in enumerate(vertex_csvs)] + [
            (f'{object_prefix}/edge{i}.csv', v) for i, v in enumerate(edge_csvs)]

        todo: List[str] = []
        for s3_object_key, v in csvs:
            # upload to s3
            with BytesIO(v) as r:
                self.upload(f=r, s3_object_key=s3_object_key)

            # now poke Neptune and tell it to load that file
            # TODO: dependencies? endpoint doesn't seem to like the way we pass these
            response = self.load(s3_object_key=s3_object_key)

            # TODO: retry?
            assert 'payload' in response and 'loadId' in response['payload'], \
                f'failed to submit load for vertex.csv: {response}'
            todo.append(response['payload']['loadId'])

        status_by_load_id: Dict[str, Mapping[str, Any]] = dict()

        while todo:
            status_by_load_id.update(
                [(id, self.load_status(load_id=id, errors=True, errors_per_page=30)['payload'])
                 for id in todo])
            todo = [load_id for load_id, overall_status in status_by_load_id.items()
                    if overall_status['overallStatus']['status'] not in ('LOAD_COMPLETED', 'LOAD_FAILED')]
            time.sleep(polling_period)

        # TODO: timeout and parse errors
        assert not todo
        failed = dict([(load_id, overall_status) for load_id, overall_status in status_by_load_id.items()
                       if overall_status['overallStatus']['status'] != 'LOAD_COMPLETED'])
        if failed:
            LOGGER.warning(f'some loads failed: {failed.keys()}: bulk_loader_details={failed}')
            if raise_if_failed:
                raise AssertionError(f'some loads failed: {failed.keys()}')

        return status_by_load_id


def group_by_class(entities: Mapping[GraphEntityType, Mapping[str, GraphEntity]]) -> \
        Tuple[Mapping[GraphEntityType, Iterable[GraphEntity]], Mapping[GraphEntityType, Iterable[GraphEntity]]]:
    vertex_types: Mapping[GraphEntityType, List[GraphEntity]] = defaultdict(list)
    edge_types: Mapping[GraphEntityType, List[GraphEntity]] = defaultdict(list)
    for t, es in entities.items():
        if isinstance(t, VertexType):
            assert not isinstance(t, EdgeType)
            vertex_types[t].extend(es.values())
        elif isinstance(t, EdgeType):
            assert not isinstance(t, VertexType)
            edge_types[t].extend(es.values())
        else:
            raise AssertionError(f'expected type {t} to be a VertexType or an EdgeType not a {type(t)}')
    return vertex_types, edge_types


def partition_properties(types: Collection[GraphEntityType]) -> Iterable[Iterable[GraphEntityType]]:
    default_cardinality: Optional[GremlinCardinality]
    if all(isinstance(t, VertexType) for t in types):
        default_cardinality = GremlinCardinality.single
    elif all(isinstance(t, EdgeType) for t in types):
        default_cardinality = None
    else:
        raise AssertionError(f'some are not VertexType or EdgeType? {types}')

    partitioned: List[Collection[GraphEntityType]] = list()

    def _try(_types: Collection[GraphEntityType]) -> None:
        by_name: Dict[str, Set[Property]] = defaultdict(set)
        by_signature: Dict[Property, Set[GraphEntityType]] = defaultdict(set)
        for t in _types:
            for p in t.properties:
                signature = p.signature(default_cardinality)
                by_name[signature.name].add(signature)
                by_signature[signature].add(t)

        overlapping_properties = [(k, v) for k, v in by_name.items() if len(v) != 1]
        if not overlapping_properties:
            partitioned.append(tuple(_types))
            return

        # this could be smarter if there are a lot of overlaps, or if you'd like to make the parititions equally sized
        ignored, signatures = overlapping_properties[0]
        overlapping_types = sorted([by_signature[signature] for signature in signatures], key=len)
        assert all(a.isdisjoint(b) for i, a in enumerate(overlapping_types) for b in overlapping_types[i + 1:]), \
            f'expected to not overlap: {overlapping_types}'

        all_overlapping_types: Set[GraphEntityType] = set().union(*overlapping_types)  # type: ignore
        other_types = set(_types).difference(all_overlapping_types)
        assert all(other_types.isdisjoint(e) for e in overlapping_types), \
            f'expected to not overlap: {other_types} and {overlapping_types}'
        overlapping_types[0] = overlapping_types[0].union(other_types)

        # let's make sure we're making progress
        assert sum(len(e) for e in overlapping_types) == len(_types) and all(len(e) > 0 for e in overlapping_types)
        for e in overlapping_types:
            _try(e)

    _try(types)

    # did we cover them all?
    assert sum(len(e) for e in partitioned) == len(types)
    # do they overlap?
    assert all(set(a).isdisjoint(set(b)) for i, a in enumerate(partitioned) for b in partitioned[i + 1:])
    return partitioned


GET = TypeVar('GET', bound=GraphEntityType)


def write_entities_as_csv(file: IO[str], entities: Mapping[GET, Iterable[GraphEntity]]) -> None:
    # TODO: also explodes if there is incompatible overlap in names, which we could avoid around by partitioning
    properties = merge_properties(entities.keys())
    formatted: List[Mapping[str, str]] = [format_entity(t, e) for t, es in entities.items() for e in es]
    write_csv(file, properties, formatted)


def merge_properties(types: Iterable[GraphEntityType]) -> PropertiesMap:
    default_cardinality: Optional[GremlinCardinality]
    if all(isinstance(t, VertexType) for t in types):
        default_cardinality = GremlinCardinality.single
    elif all(isinstance(t, EdgeType) for t in types):
        default_cardinality = None
    else:
        raise AssertionError(f'some are not VertexType or EdgeType? {types}')

    by_name: Dict[str, Set[Property]] = defaultdict(set)
    for t in types:
        for p in t.properties:
            signature = p.signature(default_cardinality)
            by_name[signature.name].add(signature)

    overlapping_types = [(k, v) for k, v in by_name.items() if len(v) != 1]
    assert not overlapping_types, f'some Property have incompatible signatures: {overlapping_types}'
    return dict([(k, v) for k, (v,) in by_name.items()])


def format_entity(entity_type: GraphEntityType, entity: GraphEntity) -> FormattedEntity:
    properties = entity_type.properties_as_map()
    # the use would naturally explode if property name isn't in the type, but this is better
    assert set(entity.keys()).issubset(set(properties.keys())), \
        f'some properties in the entity are not in the entity type? entity: {entity}, type: {entity_type}'
    assert set(entity.keys()).issuperset(set([k for k, p in properties.items() if p.required])), \
        f'some required properties in the entity are not present? entity: {entity}, type: {entity_type}'
    return dict([(n, properties[n].format(v)) for n, v in entity.items()])


# these seem to match the format that neptune-export produces (which doesn't use csv.writer)
csv_kwargs = dict(dialect='excel', delimiter=',', quotechar='"', doublequote=True)


def write_csv(file: IO[str], properties: PropertiesMap, entities: FormattedEntities) -> None:
    """
    entities is the already formatted values
    """
    # eliminate the not-present properties
    property_names_set: Set[str] = set()
    for e in entities:
        property_names_set.update(k for k, v in e.items() if v is not None)
    assert property_names_set.issubset(properties.keys()), f'wat? entities have property names not in the Properties?'
    property_names: List[str] = sorted(list(property_names_set))

    # only really shows up in testing
    if not property_names:
        return

    # no, it's not a context manager
    w = csv.writer(file, **csv_kwargs)

    # don't writeheader, instead write header from properties
    w.writerow([properties[n].header() for n in property_names])

    for e in entities:
        w.writerow([e.get(n, '') for n in property_names])


def new_entities() -> Dict[GraphEntityType, List[GraphEntity]]:
    return defaultdict(list)
