# /// script
# requires-python = ">=3.11"
# dependencies = ["fastmcp>=2.10,<3", "databricks-sdk>=0.50"]
# ///
"""Stdio proxy to a Databricks Unity Gateway MCP, authenticated via the Databricks CLI profile."""

import os

import httpx
from databricks.sdk.core import Config
from fastmcp import Client, FastMCP
from fastmcp.client.transports import StreamableHttpTransport


class DatabricksAuth(httpx.Auth):
    def __init__(self) -> None:
        # Reads DATABRICKS_CONFIG_PROFILE; the SDK caches and refreshes the token.
        self._config = Config()

    def auth_flow(self, request):
        request.headers.update(self._config.authenticate())
        yield request


transport = StreamableHttpTransport(os.environ["DATABRICKS_MCP_URL"], auth=DatabricksAuth())
FastMCP.as_proxy(Client(transport), name="databricks-mcp-proxy").run()
