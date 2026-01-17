from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from src.clients.api_client import ApiClient
from src.clients.management_client import ManagementAPIClient
from src.clients.sdk_client import SupabaseSDKClient
from src.core.feature_manager import FeatureManager
from src.logger import logger
from src.services.api.api_manager import SupabaseApiManager
from src.services.database.postgres_client import PostgresClient
from src.services.database.query_manager import QueryManager
from src.services.logs.log_manager import LogManager
from src.services.safety.safety_manager import SafetyManager
from src.settings import Settings
from src.tools import ToolManager


class ServicesContainer:
    """Container for all services"""

    _instance: ServicesContainer | None = None

    def __init__(
        self,
        mcp_server: FastMCP | None = None,
        postgres_client: PostgresClient | None = None,
        api_client: ManagementAPIClient | None = None,
        sdk_client: SupabaseSDKClient | None = None,
        api_manager: SupabaseApiManager | None = None,
        safety_manager: SafetyManager | None = None,
        query_manager: QueryManager | None = None,
        tool_manager: ToolManager | None = None,
        log_manager: LogManager | None = None,
        query_api_client: ApiClient | None = None,
        feature_manager: FeatureManager | None = None,
    ) -> None:
        """Create a new container container reference"""
        self.postgres_client = postgres_client
        self.api_client = api_client
        self.api_manager = api_manager
        self.sdk_client = sdk_client
        self.safety_manager = safety_manager
        self.query_manager = query_manager
        self.tool_manager = tool_manager
        self.log_manager = log_manager
        self.query_api_client = query_api_client
        self.feature_manager = feature_manager
        self.mcp_server = mcp_server

    @classmethod
    def get_instance(cls) -> ServicesContainer:
        """Get the singleton instance of the container"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def initialize_services(self, settings: Settings) -> None:
        """Initializes all services in a synchronous manner to satisfy MCP runtime requirements"""
        # Create clients
        self.postgres_client = PostgresClient.get_instance(settings=settings)
        self.api_client = ManagementAPIClient(settings=settings)  # not a singleton, simple
        self.sdk_client = SupabaseSDKClient.get_instance(settings=settings)

        # Create managers
        self.safety_manager = SafetyManager.get_instance()
        self.api_manager = SupabaseApiManager.get_instance(
            api_client=self.api_client,
            safety_manager=self.safety_manager,
        )
        self.query_manager = QueryManager(
            postgres_client=self.postgres_client,
            safety_manager=self.safety_manager,
        )
        self.tool_manager = ToolManager.get_instance()

        # Register safety configs
        self.safety_manager.register_safety_configs()

        # Create query api client
        self.query_api_client = ApiClient()
        self.feature_manager = FeatureManager(self.query_api_client)

        logger.info("✓ All services initialized successfully.")

    async def shutdown_services(self) -> None:
        """Properly close all relevant clients and connections"""
        # Postgres client
        if self.postgres_client:
            await self.postgres_client.close()

        # API clients
        if self.query_api_client:
            await self.query_api_client.close()

        if self.api_client:
            await self.api_client.close()

        # SDK client
        if self.sdk_client:
            await self.sdk_client.close()
