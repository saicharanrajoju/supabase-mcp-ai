import httpx
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from supabase_mcp.clients.management_client import ManagementAPIClient
from supabase_mcp.exceptions import APIClientError, APIConnectionError
from supabase_mcp.settings import Settings


@pytest.mark.asyncio(loop_scope="module")
class TestAPIClient:
    """Unit tests for the API client."""

    @pytest.fixture
    def mock_settings(self):
        """Create mock settings for testing."""
        settings = MagicMock(spec=Settings)
        settings.supabase_access_token = "test-token"
        settings.supabase_project_ref = "test-project-ref"
        settings.supabase_region = "us-east-1"
        settings.query_api_url = "https://api.test.com"
        settings.supabase_api_url = "https://api.supabase.com"
        return settings

    async def test_execute_get_request(self, mock_settings):
        """Test executing a GET request to the API."""
        # Create client but don't mock the httpx client yet
        client = ManagementAPIClient(settings=mock_settings)
        
        # Setup mock response
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 404
        mock_response.is_success = False
        mock_response.headers = {"content-type": "application/json"}
        mock_response.json.return_value = {"message": "Cannot GET /v1/health"}
        mock_response.text = '{"message": "Cannot GET /v1/health"}'
        mock_response.content = b'{"message": "Cannot GET /v1/health"}'
        
        # Mock the send_request method to return our mock response
        with patch.object(client, 'send_request', return_value=mock_response):
            path = "/v1/health"
            
            # Execute the request and expect a 404 error
            with pytest.raises(APIClientError) as exc_info:
                await client.execute_request(
                    method="GET",
                    path=path,
                )
            
            # Verify the error details
            assert exc_info.value.status_code == 404
            assert "Cannot GET /v1/health" in str(exc_info.value)

    async def test_request_preparation(self, mock_settings):
        """Test that requests are properly prepared with headers and parameters."""
        client = ManagementAPIClient(settings=mock_settings)
        
        # Prepare a request with parameters
        method = "GET"
        path = "/v1/health"
        request_params = {"param1": "value1", "param2": "value2"}

        # Prepare the request
        request = client.prepare_request(
            method=method,
            path=path,
            request_params=request_params,
        )

        # Verify the request
        assert request.method == method
        assert path in str(request.url)
        assert "param1=value1" in str(request.url)
        assert "param2=value2" in str(request.url)
        assert "Content-Type" in request.headers
        assert request.headers["Content-Type"] == "application/json"

    async def test_error_handling(self, mock_settings):
        """Test handling of API errors."""
        client = ManagementAPIClient(settings=mock_settings)
        
        # Setup mock response
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 404
        mock_response.is_success = False
        mock_response.headers = {"content-type": "application/json"}
        mock_response.json.return_value = {"message": "Cannot GET /v1/nonexistent-endpoint"}
        mock_response.text = '{"message": "Cannot GET /v1/nonexistent-endpoint"}'
        mock_response.content = b'{"message": "Cannot GET /v1/nonexistent-endpoint"}'
        
        with patch.object(client, 'send_request', return_value=mock_response):
            path = "/v1/nonexistent-endpoint"
            
            # Execute the request and expect an APIClientError
            with pytest.raises(APIClientError) as exc_info:
                await client.execute_request(
                    method="GET",
                    path=path,
                )
            
            # Verify the error details
            assert exc_info.value.status_code == 404
            assert "Cannot GET /v1/nonexistent-endpoint" in str(exc_info.value)

    async def test_request_with_body(self, mock_settings):
        """Test executing a request with a body."""
        client = ManagementAPIClient(settings=mock_settings)
        
        # Test the request preparation
        method = "POST"
        path = "/v1/health/check"
        request_body = {"test": "data", "nested": {"value": 123}}

        # Prepare the request
        request = client.prepare_request(
            method=method,
            path=path,
            request_body=request_body,
        )

        # Verify the request
        assert request.method == method
        assert path in str(request.url)
        assert request.content  # Should have content for the body
        assert "Content-Type" in request.headers
        assert request.headers["Content-Type"] == "application/json"

    async def test_response_parsing(self, mock_settings):
        """Test parsing API responses."""
        client = ManagementAPIClient(settings=mock_settings)
        
        # Setup mock response
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200
        mock_response.is_success = True
        mock_response.headers = {"content-type": "application/json"}
        mock_response.json.return_value = [{"id": "project1", "name": "Test Project"}]
        mock_response.content = b'[{"id": "project1", "name": "Test Project"}]'
        
        with patch.object(client, 'send_request', return_value=mock_response):
            path = "/v1/projects"
            
            # Execute the request
            response = await client.execute_request(
                method="GET",
                path=path,
            )
            
            # Verify the response is parsed correctly
            assert isinstance(response, list)
            assert len(response) > 0
            assert "id" in response[0]

    async def test_request_retry_mechanism(self, mock_settings):
        """Test that the tenacity retry mechanism works correctly for API requests."""
        client = ManagementAPIClient(settings=mock_settings)
        
        # Create a mock request object for the NetworkError
        mock_request = MagicMock(spec=httpx.Request)
        mock_request.method = "GET"
        mock_request.url = "https://api.supabase.com/v1/projects"
        
        # Mock the client's send method to always raise a network error
        with patch.object(client.client, 'send', side_effect=httpx.NetworkError("Simulated network failure", request=mock_request)):
            # Execute a request - this should trigger retries and eventually fail
            with pytest.raises(APIConnectionError) as exc_info:
                await client.execute_request(
                    method="GET",
                    path="/v1/projects",
                )
            
            # Verify the error message indicates retries were attempted
            assert "Network error after 3 retry attempts" in str(exc_info.value)

    async def test_request_without_access_token(self, mock_settings):
        """Test that an exception is raised when attempting to send a request without an access token."""
        # Create client with no access token
        mock_settings.supabase_access_token = None
        client = ManagementAPIClient(settings=mock_settings)

        # Attempt to execute a request - should raise an exception
        with pytest.raises(APIClientError) as exc_info:
            await client.execute_request(
                method="GET",
                path="/v1/projects",
            )
        
        assert "Supabase access token is not configured" in str(exc_info.value)