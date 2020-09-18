# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

from typing import Union
from urllib.parse import SplitResult, urlsplit


def to_aws4_request_compatible_host(url: Union[str, SplitResult]) -> str:
    """
    Why do this? well, requests-aws4auth quietly pretends a Host header exists by parsing the request URL.  Why?  In
    the python stack, requests defers adding of the host header to a lower layer library (http.client) which comes
    after the auth objects like AWS4Auth get run ...so it guesses.  However, its guess is just the host part, which
    works if you're using https and port 443 or http and port 80, but not so much if you're using https and port
    8182 for example.
    """
    if isinstance(url, str):
        result = urlsplit(url)
    elif isinstance(url, SplitResult):
        result = url
    # we have to canonicalize the URL as the server would (so omit if https and port 443 or http and port 80)
    if (result.scheme == 'https' and result.port == 443) or (result.scheme == 'http' and result.port == 80):
        return result.netloc.split(':')[0]
    else:
        return result.netloc
