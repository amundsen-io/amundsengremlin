# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import os
from typing import Any, Mapping, Optional, Union


class Config:
    pass


NEPTUNE_URLS_BY_USER: Mapping[str, Mapping[str, Any]] = {
    "nobody": {
        "neptune_endpoint": "nowhere.amazonaws.com",
        "neptune_port": 8182,
        "uri": "nowhere.amazonaws.com:8182/gremlin"
    },
}


def neptune_url_for_development(*, user: Optional[str] = None) -> Optional[Mapping[str, Any]]:
    # Hello!  If you get here and and your user is not above, ask one of them to borrow theirs. Or add your username
    # to development_instance_users in terraform/deployments/development/main.tf and terraform apply
    # TODO: add terraform files. One stopgap is to manually set up neptune instances for each dev user.
    return NEPTUNE_URLS_BY_USER[os.getenv('USER', 'nobody')]


class TestGremlinConfig(Config):
    NEPTUNE_BULK_LOADER_S3_BUCKET_NAME = 'amundsen-gremlin-development-bulk-loader'
    NEPTUNE_URL = 'something.amazonaws.com:8182/gremlin'
    # TODO: populate a session here
    NEPTUNE_SESSION = None


class LocalGremlinConfig(Config):
    LOG_LEVEL = 'DEBUG'
    NEPTUNE_BULK_LOADER_S3_BUCKET_NAME = 'amundsen-gremlin-development-bulk-loader'
    # The appropriate AWS region for your neptune setup
    # ex: AWS_REGION_NAME = 'us-west-2'
    AWS_REGION_NAME = None
    # NB: Session should be shaped like:
    # NEPTUNE_SESSION = boto3.session.Session(profile_name='youruserprofilehere',
    #                                         region_name=AWS_REGION_NAME)
    # Unfortunately this will always blow up without a legit profile name
    NEPTUNE_SESSION = None
    # NB: NEPTUNE_URL should be shaped like:
    # NEPTUNE_URL = neptune_url_for_development()
    # Unfortunately, this will blow up if your user is not present
    NEPTUNE_URL = None
    PROXY_HOST: Union[str, Mapping[str, Any], None] = NEPTUNE_URL
