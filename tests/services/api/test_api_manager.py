from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from supabase_mcp.exceptions import SafetyError
from supabase_mcp.services.api.api_manager import SupabaseApiManager
from supabase_mcp.services.safety.models import ClientType


class TestApiManager:
    """Tests for the API Manager."""

    @pytest.mark.unit
    def test_path_parameter_replacement(self, mock_api_manager: SupabaseApiManager):
        """
        Test that path parameters are correctly replaced in API paths.

        This test verifies that the API Manager correctly replaces path placeholders
        with actual values, handling both required and optional parameters.
        """
        # Use the mock_api_manager fixture instead of creating one manually
        api_manager = mock_api_manager

        # Test with a simple path and required parameters (avoiding 'ref' which is auto-injected)
        path = "/v1/organizations/{slug}/members"
        path_params = {"slug": "example-org"}

        result = api_manager.replace_path_params(path, path_params)
        expected = "/v1/organizations/example-org/members"
        assert result == expected, f"Expected {expected}, got {result}"

        # Test with missing required parameters
        path = "/v1/organizations/{slug}/members/{id}"
        path_params = {"slug": "example-org"}

        with pytest.raises(ValueError) as excinfo:
            api_manager.replace_path_params(path, path_params)
        assert "Missing path parameters" in str(excinfo.value)

        # Test with extra parameters (should be ignored)
        path = "/v1/organizations/{slug}"
        path_params = {"slug": "example-org", "extra": "should-be-ignored"}

        with pytest.raises(ValueError) as excinfo:
            api_manager.replace_path_params(path, path_params)
        assert "Unknown path parameter" in str(excinfo.value)

        # Test with no parameters
        path = "/v1/organizations"
        result = api_manager.replace_path_params(path)
        expected = "/v1/organizations"
        assert result == expected, f"Expected {expected}, got {result}"

    @pytest.mark.asyncio
    @pytest.mark.unit
    @patch("supabase_mcp.services.api.api_manager.logger")
    async def test_safety_validation(self, mock_logger: MagicMock, mock_api_manager: SupabaseApiManager):
        """
        Test that API operations are properly validated through the safety manager.

        This test verifies that the API Manager correctly validates operations
        before executing them, and handles safety errors appropriately.
        """
        # Use the mock_api_manager fixture instead of creating one manually
        api_manager = mock_api_manager

        # Mock the replace_path_params method to return the path unchanged
        api_manager.replace_path_params = MagicMock(return_value="/v1/organizations/example-org")

        # Mock the client's execute_request method to return a simple response
        mock_response = {"success": True}
        api_manager.client.execute_request = MagicMock()
        api_manager.client.execute_request.return_value = mock_response

        # Make the mock awaitable
        async def mock_execute_request(*args: Any, **kwargs: Any) -> dict[str, Any]:
            return mock_response

        api_manager.client.execute_request = mock_execute_request

        # Test a successful operation
        method = "GET"
        path = "/v1/organizations/{slug}"
        path_params = {"slug": "example-org"}

        result = await api_manager.execute_request(method, path, path_params)

        # Verify that the safety manager was called with the correct parameters
        api_manager.safety_manager.validate_operation.assert_called_once_with(
            ClientType.API, (method, path, path_params, None, None), has_confirmation=False
        )

        # Verify that the result is what we expected
        assert result == {"success": True}

        # Test an operation that fails safety validation
        api_manager.safety_manager.validate_operation.reset_mock()

        # Make the safety manager raise a SafetyError
        def raise_safety_error(*args: Any, **kwargs: Any) -> None:
            raise SafetyError("Operation not allowed")

        api_manager.safety_manager.validate_operation.side_effect = raise_safety_error

        # The execute_request method should raise the SafetyError
        with pytest.raises(SafetyError) as excinfo:
            await api_manager.execute_request("DELETE", "/v1/organizations/{slug}", {"slug": "example-org"})

        assert "Operation not allowed" in str(excinfo.value)

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_retrieve_logs_basic(self, mock_api_manager: SupabaseApiManager):
        """
        Test that the retrieve_logs method correctly builds and executes a logs query.

        This test verifies that the API Manager correctly builds a logs query using
        the LogManager and executes it through the Management API.
        """
        # Mock the log_manager's build_logs_query method
        mock_api_manager.log_manager.build_logs_query = MagicMock(return_value="SELECT * FROM postgres_logs LIMIT 10")

        # Mock the execute_request method to return a simple response
        mock_response = {"result": [{"id": "123", "event_message": "test"}]}

        async def mock_execute_request(*args: Any, **kwargs: Any) -> dict[str, Any]:
            return mock_response

        mock_api_manager.execute_request = mock_execute_request

        # Execute the method
        result = await mock_api_manager.retrieve_logs(
            collection="postgres",
            limit=10,
            hours_ago=24,
        )

        # Verify that the log_manager was called with the correct parameters
        mock_api_manager.log_manager.build_logs_query.assert_called_once_with(
            collection="postgres",
            limit=10,
            hours_ago=24,
            filters=None,
            search=None,
            custom_query=None,
        )

        # Verify that the result is what we expected
        assert result == {"result": [{"id": "123", "event_message": "test"}]}

    @pytest.mark.asyncio
    @pytest.mark.unit
    async def test_retrieve_logs_error_handling(self, mock_api_manager: SupabaseApiManager):
        """
        Test that the retrieve_logs method correctly handles errors.

        This test verifies that the API Manager correctly handles errors that occur
        during log retrieval and propagates them to the caller.
        """
        # Mock the log_manager's build_logs_query method
        mock_api_manager.log_manager.build_logs_query = MagicMock(return_value="SELECT * FROM postgres_logs LIMIT 10")

        # Mock the execute_request method to raise an exception
        async def mock_execute_request_error(*args: Any, **kwargs: Any) -> dict[str, Any]:
            raise Exception("API error")

        mock_api_manager.execute_request = mock_execute_request_error

        # The retrieve_logs method should propagate the exception
        with pytest.raises(Exception) as excinfo:
            await mock_api_manager.retrieve_logs(collection="postgres")

        assert "API error" in str(excinfo.value)
