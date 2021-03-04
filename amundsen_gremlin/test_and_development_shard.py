# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import os
from threading import Lock
from typing import Optional

from gremlin_python.process.graph_traversal import GraphTraversalSource

# use _get_shard to retrieve this
_shard: Optional[str]
_shard_lock = Lock()
_shard_used = False


def _shard_default() -> Optional[str]:
    if os.environ.get('CI'):
        # TODO: support CI-specific env variables in config?
        #   BUILD_PART_ID: identifies a part-build (doesn't change when you click rebuild, but also not shared across
        #     builds)
        build_part_id = os.environ.get('BUILD_PART_ID')
        # TBD: can we easily shard on github?
        if build_part_id is None:
            return 'OneShardToRuleThemAll'
        assert build_part_id, f'Expected BUILD_PART_ID environment variable to be set'

        # e.g. gw0 if -n or main if -n0
        xdist_worker = os.environ.get('PYTEST_XDIST_WORKER')

        if xdist_worker:
            return f'{build_part_id}_{xdist_worker}'
        else:
            return build_part_id
    elif os.environ.get('DATACENTER', 'local') == 'local':
        # this replaces the NEPTUNE_URLS_BY_USER et al in Development.
        user = os.environ.get('USER', 'test_user')
        assert user is not None, f'Expected USER environment variable to be set'

        # e.g. gw0 if -n or main if -n0
        xdist_worker = os.environ.get('PYTEST_XDIST_WORKER')

        if xdist_worker:
            return f'{user}_{xdist_worker}'
        else:
            return user
    else:
        return None


def shard_set_explicitly(shard: Optional[str]) -> None:
    global _shard
    with _shard_lock:
        assert not _shard_used, 'can only shard_set_explicitly if it has not been used yet.  (sorry)'
        _shard = shard


def get_shard() -> Optional[str]:
    global _shard, _shard_used
    # lock free path first
    if _shard_used:
        return _shard
    with _shard_lock:
        _shard_used = True
    return _shard


def _reset_for_testing_only() -> None:
    global _shard, _shard_used, get_shard
    _shard = _shard_default()
    _shard_used = False


# or just the once here
_reset_for_testing_only()


def delete_graph_for_shard_only(g: GraphTraversalSource) -> None:
    shard = get_shard()
    assert shard, f'expected shard to exist! Surely you are only using this in development or test?'
    # TODO: do something better than not using WellKnownProperties.TestShard here (since that makes a circular
    # dependency)
    g.V().has('shard', shard).drop().iterate()
