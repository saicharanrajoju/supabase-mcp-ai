import time
import uuid
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from supabase_mcp.clients.sdk_client import SupabaseSDKClient
from supabase_mcp.exceptions import PythonSDKError
from supabase_mcp.settings import Settings

# Unique identifier for test users to avoid conflicts
TEST_ID = f"test-{int(time.time())}-{uuid.uuid4().hex[:6]}"


# Create unique test emails
def get_test_email(prefix: str = "user"):
    """Generate a unique test email"""
    return f"a.zuev+{prefix}-{TEST_ID}@outlook.com"


@pytest.mark.asyncio(loop_scope="module")
class TestSDKClientIntegration:
    """
    Unit tests for the SupabaseSDKClient.
    """

    @pytest.fixture
    def mock_settings(self):
        """Create mock settings for testing."""
        settings = MagicMock(spec=Settings)
        settings.supabase_project_ref = "test-project-ref"
        settings.supabase_service_role_key = "test-service-role-key"
        settings.supabase_region = "us-east-1"
        settings.supabase_url = "https://test-project-ref.supabase.co"
        return settings

    @pytest.fixture
    async def mock_sdk_client(self, mock_settings):
        """Create a mock SDK client for testing."""
        # Reset singleton
        SupabaseSDKClient.reset()
        
        # Mock the Supabase client
        mock_supabase = MagicMock()
        mock_auth_admin = MagicMock()
        mock_supabase.auth.admin = mock_auth_admin
        
        # Mock the create_async_client function to return our mock client
        with patch('supabase_mcp.clients.sdk_client.create_async_client', return_value=mock_supabase):
            # Create client - this will now use our mocked create_async_client
            client = SupabaseSDKClient.get_instance(settings=mock_settings)
            # Manually set the client to ensure it's available
            client.client = mock_supabase
            
        return client

    async def test_list_users(self, mock_sdk_client: SupabaseSDKClient):
        """Test listing users with pagination"""
        # Mock user data
        mock_users = [
            MagicMock(id="user1", email="user1@test.com", user_metadata={}),
            MagicMock(id="user2", email="user2@test.com", user_metadata={})
        ]
        
        # Mock the list_users method as an async function
        mock_sdk_client.client.auth.admin.list_users = AsyncMock(return_value=mock_users)
        
        # Create test parameters
        list_params = {"page": 1, "per_page": 10}

        # List users
        result = await mock_sdk_client.call_auth_admin_method("list_users", list_params)

        # Verify response format
        assert result is not None
        assert hasattr(result, "__iter__")  # Should be iterable (list of users)
        assert len(result) == 2

        # Check that the first user has expected attributes
        first_user = result[0]
        assert hasattr(first_user, "id")
        assert hasattr(first_user, "email")
        assert hasattr(first_user, "user_metadata")

        # Test with invalid parameters - mock the validation error
        mock_sdk_client.client.auth.admin.list_users = AsyncMock(side_effect=Exception("Bad Pagination Parameters"))
        
        invalid_params = {"page": -1, "per_page": 10}
        with pytest.raises(PythonSDKError) as excinfo:
            await mock_sdk_client.call_auth_admin_method("list_users", invalid_params)

        assert "Bad Pagination Parameters" in str(excinfo.value)

    async def test_get_user_by_id(self, mock_sdk_client: SupabaseSDKClient):
        """Test retrieving a user by ID"""
        # Mock user data
        test_email = get_test_email("get")
        user_id = str(uuid.uuid4())
        
        mock_user = MagicMock(
            id=user_id,
            email=test_email,
            user_metadata={"name": "Test User", "test_id": TEST_ID}
        )
        mock_response = MagicMock(user=mock_user)
        
        # Mock the get_user_by_id method as an async function
        mock_sdk_client.client.auth.admin.get_user_by_id = AsyncMock(return_value=mock_response)
        
        # Get the user by ID
        get_params = {"uid": user_id}
        get_result = await mock_sdk_client.call_auth_admin_method("get_user_by_id", get_params)

        # Verify user data
        assert get_result is not None
        assert hasattr(get_result, "user")
        assert get_result.user.id == user_id
        assert get_result.user.email == test_email

        # Test with invalid parameters (non-existent user ID)
        mock_sdk_client.client.auth.admin.get_user_by_id = AsyncMock(side_effect=Exception("user_id must be an UUID"))
        
        invalid_params = {"uid": "non-existent-user-id"}
        with pytest.raises(PythonSDKError) as excinfo:
            await mock_sdk_client.call_auth_admin_method("get_user_by_id", invalid_params)

        assert "user_id must be an UUID" in str(excinfo.value)

    async def test_create_user(self, mock_sdk_client: SupabaseSDKClient):
        """Test creating a new user"""
        # Create a new test user
        test_email = get_test_email("create")
        user_id = str(uuid.uuid4())
        
        mock_user = MagicMock(
            id=user_id,
            email=test_email,
            user_metadata={"name": "Test User", "test_id": TEST_ID}
        )
        mock_response = MagicMock(user=mock_user)
        
        # Mock the create_user method as an async function
        mock_sdk_client.client.auth.admin.create_user = AsyncMock(return_value=mock_response)
        
        create_params = {
            "email": test_email,
            "password": f"Password123!{TEST_ID}",
            "email_confirm": True,
            "user_metadata": {"name": "Test User", "test_id": TEST_ID},
        }

        # Create the user
        create_result = await mock_sdk_client.call_auth_admin_method("create_user", create_params)
        assert create_result is not None
        assert hasattr(create_result, "user")
        assert hasattr(create_result.user, "id")
        assert create_result.user.id == user_id

        # Test with invalid parameters (missing required fields)
        mock_sdk_client.client.auth.admin.create_user = AsyncMock(side_effect=Exception("Invalid parameters"))
        
        invalid_params = {"user_metadata": {"name": "Invalid User"}}
        with pytest.raises(PythonSDKError) as excinfo:
            await mock_sdk_client.call_auth_admin_method("create_user", invalid_params)

        assert "Invalid parameters" in str(excinfo.value)

    async def test_update_user_by_id(self, mock_sdk_client: SupabaseSDKClient):
        """Test updating a user's attributes"""
        # Mock user data
        test_email = get_test_email("update")
        user_id = str(uuid.uuid4())
        
        mock_user = MagicMock(
            id=user_id,
            email=test_email,
            user_metadata={"email": "afterupdated@email.com"}
        )
        mock_response = MagicMock(user=mock_user)
        
        # Mock the update_user_by_id method as an async function
        mock_sdk_client.client.auth.admin.update_user_by_id = AsyncMock(return_value=mock_response)
        
        # Update the user
        update_params = {
            "uid": user_id,
            "attributes": {
                "user_metadata": {
                    "email": "afterupdated@email.com",
                }
            },
        }

        update_result = await mock_sdk_client.call_auth_admin_method("update_user_by_id", update_params)

        # Verify user was updated
        assert update_result is not None
        assert hasattr(update_result, "user")
        assert update_result.user.id == user_id
        assert update_result.user.user_metadata["email"] == "afterupdated@email.com"

        # Test with invalid parameters (non-existent user ID)
        mock_sdk_client.client.auth.admin.update_user_by_id = AsyncMock(side_effect=Exception("user_id must be an uuid"))
        
        invalid_params = {
            "uid": "non-existent-user-id",
            "attributes": {"user_metadata": {"name": "Invalid Update"}},
        }
        with pytest.raises(PythonSDKError) as excinfo:
            await mock_sdk_client.call_auth_admin_method("update_user_by_id", invalid_params)

        assert "user_id must be an uuid" in str(excinfo.value).lower()

    async def test_delete_user(self, mock_sdk_client: SupabaseSDKClient):
        """Test deleting a user"""
        # Mock user data
        user_id = str(uuid.uuid4())
        
        # Mock the delete_user method as an async function to return None (success)
        mock_sdk_client.client.auth.admin.delete_user = AsyncMock(return_value=None)
        
        # Delete the user
        delete_params = {"id": user_id}
        # The delete_user method returns None on success
        result = await mock_sdk_client.call_auth_admin_method("delete_user", delete_params)
        assert result is None

        # Test with invalid parameters (non-UUID format user ID)
        mock_sdk_client.client.auth.admin.delete_user = AsyncMock(side_effect=Exception("user_id must be an uuid"))
        
        invalid_params = {"id": "non-existent-user-id"}
        with pytest.raises(PythonSDKError) as excinfo:
            await mock_sdk_client.call_auth_admin_method("delete_user", invalid_params)

        assert "user_id must be an uuid" in str(excinfo.value).lower()

    async def test_invite_user_by_email(self, mock_sdk_client: SupabaseSDKClient):
        """Test inviting a user by email"""
        # Mock user data
        test_email = get_test_email("invite")
        user_id = str(uuid.uuid4())
        
        mock_user = MagicMock(
            id=user_id,
            email=test_email,
            invited_at=datetime.now().isoformat()
        )
        mock_response = MagicMock(user=mock_user)
        
        # Mock the invite_user_by_email method as an async function
        mock_sdk_client.client.auth.admin.invite_user_by_email = AsyncMock(return_value=mock_response)
        
        # Create invite parameters
        invite_params = {
            "email": test_email,
            "options": {"data": {"name": "Invited User", "test_id": TEST_ID, "invited_at": datetime.now().isoformat()}},
        }

        # Invite the user
        result = await mock_sdk_client.call_auth_admin_method("invite_user_by_email", invite_params)

        # Verify response
        assert result is not None
        assert hasattr(result, "user")
        assert result.user.email == test_email
        assert hasattr(result.user, "invited_at")

        # Test with invalid parameters (missing email)
        mock_sdk_client.client.auth.admin.invite_user_by_email = AsyncMock(side_effect=Exception("Invalid parameters"))
        
        invalid_params = {"options": {"data": {"name": "Invalid Invite"}}}
        with pytest.raises(PythonSDKError) as excinfo:
            await mock_sdk_client.call_auth_admin_method("invite_user_by_email", invalid_params)

        assert "Invalid parameters" in str(excinfo.value)

    async def test_generate_link(self, mock_sdk_client: SupabaseSDKClient):
        """Test generating authentication links"""
        # Mock response for generate_link
        mock_properties = MagicMock(action_link="https://example.com/auth/link")
        mock_response = MagicMock(properties=mock_properties)
        
        # Mock the generate_link method as an async function
        mock_sdk_client.client.auth.admin.generate_link = AsyncMock(return_value=mock_response)
        
        # Test signup link
        link_params = {
            "type": "signup",
            "email": get_test_email("signup"),
            "password": f"Password123!{TEST_ID}",
            "options": {
                "data": {"name": "Signup User", "test_id": TEST_ID},
                "redirect_to": "https://example.com/welcome",
            },
        }

        # Generate link
        result = await mock_sdk_client.call_auth_admin_method("generate_link", link_params)

        # Verify response
        assert result is not None
        assert hasattr(result, "properties")
        assert hasattr(result.properties, "action_link")

        # Test with invalid parameters (invalid link type)
        mock_sdk_client.client.auth.admin.generate_link = AsyncMock(side_effect=Exception("Invalid parameters"))
        
        invalid_params = {"type": "invalid_type", "email": get_test_email("invalid")}
        with pytest.raises(PythonSDKError) as excinfo:
            await mock_sdk_client.call_auth_admin_method("generate_link", invalid_params)

        assert "Invalid parameters" in str(excinfo.value) or "invalid type" in str(excinfo.value).lower()

    async def test_delete_factor(self, mock_sdk_client: SupabaseSDKClient):
        """Test deleting an MFA factor"""
        # Mock the delete_factor method as an async function to raise not implemented
        mock_sdk_client.client.auth.admin.delete_factor = AsyncMock(side_effect=AttributeError("method not found"))
        
        # Attempt to delete a factor
        delete_factor_params = {"user_id": str(uuid.uuid4()), "id": "non-existent-factor-id"}

        with pytest.raises(PythonSDKError) as excinfo:
            await mock_sdk_client.call_auth_admin_method("delete_factor", delete_factor_params)
        
        # We expect this to fail with a specific error message
        assert "not implemented" in str(excinfo.value).lower() or "method not found" in str(excinfo.value).lower()

    async def test_empty_parameters(self, mock_sdk_client: SupabaseSDKClient):
        """Test validation errors with empty parameters for various methods"""
        # Test methods with empty parameters
        methods = ["get_user_by_id", "create_user", "update_user_by_id", "delete_user", "generate_link"]

        for method in methods:
            empty_params = {}
            
            # Mock the method to raise validation error
            setattr(mock_sdk_client.client.auth.admin, method, AsyncMock(side_effect=Exception("Invalid parameters")))

            # Should raise PythonSDKError containing validation error details
            with pytest.raises(PythonSDKError) as excinfo:
                await mock_sdk_client.call_auth_admin_method(method, empty_params)

            # Verify error message contains validation details
            assert "Invalid parameters" in str(excinfo.value) or "validation error" in str(excinfo.value).lower()

    async def test_client_without_service_role_key(self, mock_settings):
        """Test that an exception is raised when attempting to use the SDK client without a service role key."""
        # Create settings without service role key
        mock_settings.supabase_service_role_key = None
        
        # Reset singleton
        SupabaseSDKClient.reset()
        
        # Create client
        client = SupabaseSDKClient.get_instance(settings=mock_settings)

        # Attempt to call a method - should raise an exception
        with pytest.raises(PythonSDKError) as excinfo:
            await client.call_auth_admin_method("list_users", {})
            
        assert "service role key is not configured" in str(excinfo.value)