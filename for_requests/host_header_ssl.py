# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
like requests_toolbelt.adapters.host_header_ssl, but with our fix

  https://github.com/requests/toolbelt/pull/289

..for:

  https://github.com/requests/toolbelt/issues/288
"""

from typing import Any

import requests


class HostHeaderSSLAdapter(requests.adapters.HTTPAdapter):
    """
    A HTTPS Adapter for Python Requests that sets the hostname for certificate
    verification based on the Host header.

    This allows requesting the IP address directly via HTTPS without getting
    a "hostname doesn't match" exception.

    Example usage:

        >>> s.mount('https://', HostHeaderSSLAdapter())
        >>> s.get("https://93.184.216.34", headers={"Host": "example.org"})

    """

    def send(self, request: requests.PreparedRequest, *args: Any, **kwargs: Any) -> requests.Response:
        # HTTP headers are case-insensitive (RFC 7230)
        host_header = None
        for header in request.headers:
            if header.lower() == "host":
                host_header = request.headers[header]
                break

        connection_pool_kwargs = self.poolmanager.connection_pool_kw

        if host_header:
            # host header can include port, but we should not include it in the
            # assert_hostname
            host_header = host_header.split(':')[0]

            connection_pool_kwargs["assert_hostname"] = host_header
        elif "assert_hostname" in connection_pool_kwargs:
            # an assert_hostname from a previous request may have been left
            connection_pool_kwargs.pop("assert_hostname", None)

        return super(HostHeaderSSLAdapter, self).send(request, *args, **kwargs)
