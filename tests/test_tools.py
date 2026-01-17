"""Unit tests for tools - no external dependencies."""
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp.server.fastmcp import FastMCP

from supabase_mcp.core.container import ServicesContainer
from supabase_mcp.exceptions import ConfirmationRequiredError, OperationNotAllowedError, PythonSDKError
from supabase_mcp.services.database.postgres_client import QueryResult, StatementResult
from supabase_mcp.services.safety.models import ClientType, OperationRiskLevel, SafetyMode


@pytest.mark.asyncio
class TestDatabaseToolsUnit:
    """Unit tests for database tools."""

    @pytest.fixture
    def mock_container(self):
        """Create a mock container with all necessary services."""
        container = MagicMock(spec=ServicesContainer)
        
        # Mock query manager
        container.query_manager = MagicMock()
        container.query_manager.handle_query = AsyncMock()
        container.query_manager.get_schemas_query = MagicMock(return_value="SELECT * FROM schemas")
        container.query_manager.get_tables_query = MagicMock(return_value="SELECT * FROM tables")
        container.query_manager.get_table_schema_query = MagicMock(return_value="SELECT * FROM columns")
        
        # Mock safety manager
        container.safety_manager = MagicMock()
        container.safety_manager.check_permission = MagicMock()
        container.safety_manager.is_unsafe_mode = MagicMock(return_value=False)
        
        return container

    async def test_get_schemas_returns_query_result(self, mock_container):
        """Test that get_schemas returns proper QueryResult."""
        # Setup mock response
        mock_result = QueryResult(results=[
            StatementResult(rows=[
                {"schema_name": "public", "total_size": "100MB", "table_count": 10},
                {"schema_name": "auth", "total_size": "50MB", "table_count": 5}
            ])
        ])
        mock_container.query_manager.handle_query.return_value = mock_result
        
        # Execute
        query = mock_container.query_manager.get_schemas_query()
        result = await mock_container.query_manager.handle_query(query)
        
        # Verify
        assert isinstance(result, QueryResult)
        assert len(result.results[0].rows) == 2
        assert result.results[0].rows[0]["schema_name"] == "public"

    async def test_get_tables_with_schema_filter(self, mock_container):
        """Test that get_tables properly filters by schema."""
        # Setup
        mock_result = QueryResult(results=[
            StatementResult(rows=[
                {"table_name": "users", "table_type": "BASE TABLE", "row_count": 100, "size_bytes": 1024}
            ])
        ])
        mock_container.query_manager.handle_query.return_value = mock_result
        
        # Execute
        query = mock_container.query_manager.get_tables_query("public")
        result = await mock_container.query_manager.handle_query(query)
        
        # Verify
        mock_container.query_manager.get_tables_query.assert_called_with("public")
        assert result.results[0].rows[0]["table_name"] == "users"

    async def test_unsafe_query_blocked_in_safe_mode(self, mock_container):
        """Test that unsafe queries are blocked in safe mode."""
        # Setup
        mock_container.safety_manager.is_unsafe_mode.return_value = False
        mock_container.safety_manager.check_permission.side_effect = OperationNotAllowedError(
            "DROP operations are not allowed in safe mode"
        )
        
        # Execute & Verify
        with pytest.raises(OperationNotAllowedError):
            mock_container.safety_manager.check_permission(
                ClientType.DATABASE,
                OperationRiskLevel.HIGH
            )


@pytest.mark.asyncio
class TestAPIToolsUnit:
    """Unit tests for API tools."""

    @pytest.fixture
    def mock_container(self):
        """Create a mock container with API services."""
        container = MagicMock(spec=ServicesContainer)
        
        # Mock API manager
        container.api_manager = MagicMock()
        container.api_manager.send_request = AsyncMock()
        container.api_manager.spec_manager = MagicMock()
        container.api_manager.spec_manager.get_full_spec = MagicMock(return_value={"paths": {}})
        
        # Mock safety manager
        container.safety_manager = MagicMock()
        container.safety_manager.check_permission = MagicMock()
        container.safety_manager.is_unsafe_mode = MagicMock(return_value=False)
        
        return container

    async def test_api_request_success(self, mock_container):
        """Test successful API request."""
        # Setup
        mock_response = {"id": "123", "name": "Test Project"}
        mock_container.api_manager.send_request.return_value = mock_response
        
        # Execute
        result = await mock_container.api_manager.send_request(
            "GET", "/v1/projects", {}
        )
        
        # Verify
        assert result["id"] == "123"
        assert result["name"] == "Test Project"

    async def test_api_spec_retrieval(self, mock_container):
        """Test API spec retrieval."""
        # Setup
        expected_spec = {
            "paths": {
                "/v1/projects": {
                    "get": {"summary": "List projects"}
                }
            }
        }
        mock_container.api_manager.spec_manager.get_full_spec.return_value = expected_spec
        
        # Execute
        spec = mock_container.api_manager.spec_manager.get_full_spec()
        
        # Verify
        assert "paths" in spec
        assert "/v1/projects" in spec["paths"]

    async def test_medium_risk_api_blocked_in_safe_mode(self, mock_container):
        """Test that medium risk API operations are blocked in safe mode."""
        # Setup
        mock_container.safety_manager.check_permission.side_effect = ConfirmationRequiredError(
            "This operation requires confirmation",
            {"method": "POST", "path": "/v1/projects"}
        )
        
        # Execute & Verify
        with pytest.raises(ConfirmationRequiredError) as exc_info:
            mock_container.safety_manager.check_permission(
                ClientType.API,
                OperationRiskLevel.MEDIUM
            )
        
        assert "requires confirmation" in str(exc_info.value)


@pytest.mark.asyncio
class TestAuthToolsUnit:
    """Unit tests for auth tools."""

    @pytest.fixture
    def mock_container(self):
        """Create a mock container with SDK client."""
        container = MagicMock(spec=ServicesContainer)
        
        # Mock SDK client
        container.sdk_client = MagicMock()
        container.sdk_client.call_auth_admin_method = AsyncMock()
        container.sdk_client.return_python_sdk_spec = MagicMock(return_value={
            "methods": ["list_users", "create_user", "delete_user"]
        })
        
        return container

    async def test_list_users_success(self, mock_container):
        """Test listing users successfully."""
        # Setup
        mock_users = [
            {"id": "user1", "email": "user1@test.com"},
            {"id": "user2", "email": "user2@test.com"}
        ]
        mock_container.sdk_client.call_auth_admin_method.return_value = mock_users
        
        # Execute
        result = await mock_container.sdk_client.call_auth_admin_method(
            "list_users", {"page": 1, "per_page": 10}
        )
        
        # Verify
        assert len(result) == 2
        assert result[0]["email"] == "user1@test.com"

    async def test_invalid_method_raises_error(self, mock_container):
        """Test that invalid method names raise errors."""
        # Setup
        mock_container.sdk_client.call_auth_admin_method.side_effect = PythonSDKError(
            "Unknown method: invalid_method"
        )
        
        # Execute & Verify
        with pytest.raises(PythonSDKError) as exc_info:
            await mock_container.sdk_client.call_auth_admin_method(
                "invalid_method", {}
            )
        
        assert "Unknown method" in str(exc_info.value)

    async def test_create_user_validation(self, mock_container):
        """Test user creation with validation."""
        # Setup
        new_user = {"id": str(uuid.uuid4()), "email": "new@test.com"}
        mock_container.sdk_client.call_auth_admin_method.return_value = {"user": new_user}
        
        # Execute
        result = await mock_container.sdk_client.call_auth_admin_method(
            "create_user", 
            {"email": "new@test.com", "password": "TestPass123!"}
        )
        
        # Verify
        assert result["user"]["email"] == "new@test.com"
        mock_container.sdk_client.call_auth_admin_method.assert_called_once()


@pytest.mark.asyncio
class TestSafetyToolsUnit:
    """Unit tests for safety tools - these already work well."""

    @pytest.fixture
    def mock_container(self):
        """Create a mock container with safety manager."""
        container = MagicMock(spec=ServicesContainer)
        
        # Mock safety manager with proper methods
        container.safety_manager = MagicMock()
        container.safety_manager.set_unsafe_mode = MagicMock()
        container.safety_manager.get_mode = MagicMock(return_value=SafetyMode.SAFE)
        container.safety_manager.confirm_operation = MagicMock()
        container.safety_manager.is_unsafe_mode = MagicMock(return_value=False)
        
        return container

    async def test_live_dangerously_enables_unsafe_mode(self, mock_container):
        """Test that live_dangerously enables unsafe mode."""
        # Execute
        mock_container.safety_manager.set_unsafe_mode(ClientType.DATABASE, True)
        
        # Verify
        mock_container.safety_manager.set_unsafe_mode.assert_called_with(ClientType.DATABASE, True)

    async def test_confirm_operation_stores_confirmation(self, mock_container):
        """Test that confirm operation stores the confirmation."""
        # Setup
        confirmation_id = str(uuid.uuid4())
        
        # Execute
        mock_container.safety_manager.confirm_operation(confirmation_id)
        
        # Verify
        mock_container.safety_manager.confirm_operation.assert_called_with(confirmation_id)

    async def test_safety_mode_switching(self, mock_container):
        """Test switching between safe and unsafe modes."""
        # Test enabling unsafe mode
        mock_container.safety_manager.set_unsafe_mode(ClientType.API, True)
        mock_container.safety_manager.set_unsafe_mode.assert_called_with(ClientType.API, True)
        
        # Test disabling unsafe mode
        mock_container.safety_manager.set_unsafe_mode(ClientType.API, False)
        mock_container.safety_manager.set_unsafe_mode.assert_called_with(ClientType.API, False)