# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
Credit to https://github.com/dwfreed in https://github.com/requests/toolbelt/issues/159
"""
import ssl
from typing import Any


class OverrideServerHostnameSSLContext(ssl.SSLContext):
    def __init__(self, *args: Any, server_hostname: str, **kwargs: Any) -> None:
        super(OverrideServerHostnameSSLContext, self).__init__(*args, **kwargs)
        self.override_server_hostname = server_hostname

    def change_server_hostname(self, server_hostname: str) -> None:
        self.override_server_hostname = server_hostname

    def wrap_socket(self, *args: Any, **kwargs: Any) -> Any:
        kwargs['server_hostname'] = self.override_server_hostname
        return super(OverrideServerHostnameSSLContext, self).wrap_socket(*args, **kwargs)
