import pytest
from pydantic import ValidationError

from supabase_mcp.services.sdk.auth_admin_models import (
    PARAM_MODELS,
    AdminUserAttributes,
    CreateUserParams,
    DeleteFactorParams,
    DeleteUserParams,
    GenerateLinkParams,
    GetUserByIdParams,
    InviteUserByEmailParams,
    ListUsersParams,
    UpdateUserByIdParams,
    UserMetadata,
)


class TestModelConversion:
    """Test conversion from JSON data to models and validation"""

    def test_get_user_by_id_conversion(self):
        """Test conversion of get_user_by_id JSON data"""
        # Valid payload
        valid_payload = {"uid": "d0e8c69f-e0c3-4a1c-b6d6-9a6c756a6a4b"}
        params = GetUserByIdParams.model_validate(valid_payload)
        assert params.uid == valid_payload["uid"]

        # Invalid payload (missing required uid)
        invalid_payload = {}
        with pytest.raises(ValidationError) as excinfo:
            GetUserByIdParams.model_validate(invalid_payload)
        assert "uid" in str(excinfo.value)

    def test_list_users_conversion(self):
        """Test conversion of list_users JSON data"""
        # Valid payload with custom values
        valid_payload = {"page": 2, "per_page": 20}
        params = ListUsersParams.model_validate(valid_payload)
        assert params.page == valid_payload["page"]
        assert params.per_page == valid_payload["per_page"]

        # Valid payload with defaults
        empty_payload = {}
        params = ListUsersParams.model_validate(empty_payload)
        assert params.page == 1
        assert params.per_page == 50

        # Invalid payload (non-integer values)
        invalid_payload = {"page": "not-a-number", "per_page": "also-not-a-number"}
        with pytest.raises(ValidationError) as excinfo:
            ListUsersParams.model_validate(invalid_payload)
        assert "page" in str(excinfo.value)

    def test_create_user_conversion(self):
        """Test conversion of create_user JSON data"""
        # Valid payload with email
        valid_payload = {
            "email": "test@example.com",
            "password": "secure-password",
            "email_confirm": True,
            "user_metadata": UserMetadata(email="test@example.com"),
        }
        params = CreateUserParams.model_validate(valid_payload)
        assert params.email == valid_payload["email"]
        assert params.password == valid_payload["password"]
        assert params.email_confirm is True
        assert params.user_metadata == valid_payload["user_metadata"]

        # Valid payload with phone
        valid_phone_payload = {
            "phone": "+1234567890",
            "password": "secure-password",
            "phone_confirm": True,
        }
        params = CreateUserParams.model_validate(valid_phone_payload)
        assert params.phone == valid_phone_payload["phone"]
        assert params.password == valid_phone_payload["password"]
        assert params.phone_confirm is True

        # Invalid payload (missing both email and phone)
        invalid_payload = {"password": "secure-password"}
        with pytest.raises(ValidationError) as excinfo:
            CreateUserParams.model_validate(invalid_payload)
        assert "Either email or phone must be provided" in str(excinfo.value)

    def test_delete_user_conversion(self):
        """Test conversion of delete_user JSON data"""
        # Valid payload with custom values
        valid_payload = {"id": "d0e8c69f-e0c3-4a1c-b6d6-9a6c756a6a4b", "should_soft_delete": True}
        params = DeleteUserParams.model_validate(valid_payload)
        assert params.id == valid_payload["id"]
        assert params.should_soft_delete is True

        # Valid payload with defaults
        valid_payload = {"id": "d0e8c69f-e0c3-4a1c-b6d6-9a6c756a6a4b"}
        params = DeleteUserParams.model_validate(valid_payload)
        assert params.id == valid_payload["id"]
        assert params.should_soft_delete is False

        # Invalid payload (missing id)
        invalid_payload = {"should_soft_delete": True}
        with pytest.raises(ValidationError) as excinfo:
            DeleteUserParams.model_validate(invalid_payload)
        assert "id" in str(excinfo.value)

    def test_invite_user_by_email_conversion(self):
        """Test conversion of invite_user_by_email JSON data"""
        # Valid payload with options
        valid_payload = {
            "email": "invite@example.com",
            "options": {"data": {"name": "Invited User"}, "redirect_to": "https://example.com/welcome"},
        }
        params = InviteUserByEmailParams.model_validate(valid_payload)
        assert params.email == valid_payload["email"]
        assert params.options == valid_payload["options"]

        # Valid payload without options
        valid_payload = {"email": "invite@example.com"}
        params = InviteUserByEmailParams.model_validate(valid_payload)
        assert params.email == valid_payload["email"]
        assert params.options is None

        # Invalid payload (missing email)
        invalid_payload = {"options": {"data": {"name": "Invited User"}}}
        with pytest.raises(ValidationError) as excinfo:
            InviteUserByEmailParams.model_validate(invalid_payload)
        assert "email" in str(excinfo.value)

    def test_generate_link_conversion(self):
        """Test conversion of generate_link JSON data"""
        # Valid signup link payload
        valid_signup_payload = {
            "type": "signup",
            "email": "user@example.com",
            "password": "secure-password",
            "options": {"data": {"name": "New User"}, "redirect_to": "https://example.com/welcome"},
        }
        params = GenerateLinkParams.model_validate(valid_signup_payload)
        assert params.type == valid_signup_payload["type"]
        assert params.email == valid_signup_payload["email"]
        assert params.password == valid_signup_payload["password"]
        assert params.options == valid_signup_payload["options"]

        # Valid email_change link payload
        valid_email_change_payload = {
            "type": "email_change_current",
            "email": "user@example.com",
            "new_email": "new@example.com",
        }
        params = GenerateLinkParams.model_validate(valid_email_change_payload)
        assert params.type == valid_email_change_payload["type"]
        assert params.email == valid_email_change_payload["email"]
        assert params.new_email == valid_email_change_payload["new_email"]

        # Invalid payload (missing password for signup)
        invalid_signup_payload = {
            "type": "signup",
            "email": "user@example.com",
        }
        with pytest.raises(ValidationError) as excinfo:
            GenerateLinkParams.model_validate(invalid_signup_payload)
        assert "Password is required for signup links" in str(excinfo.value)

        # Invalid payload (missing new_email for email_change)
        invalid_email_change_payload = {
            "type": "email_change_current",
            "email": "user@example.com",
        }
        with pytest.raises(ValidationError) as excinfo:
            GenerateLinkParams.model_validate(invalid_email_change_payload)
        assert "new_email is required for email change links" in str(excinfo.value)

        # Invalid payload (invalid type)
        invalid_type_payload = {
            "type": "invalid-type",
            "email": "user@example.com",
        }
        with pytest.raises(ValidationError) as excinfo:
            GenerateLinkParams.model_validate(invalid_type_payload)
        assert "type" in str(excinfo.value)

    def test_update_user_by_id_conversion(self):
        """Test conversion of update_user_by_id JSON data"""
        # Valid payload
        valid_payload = {
            "uid": "d0e8c69f-e0c3-4a1c-b6d6-9a6c756a6a4b",
            "attributes": AdminUserAttributes(email="updated@example.com", email_verified=True),
        }
        params = UpdateUserByIdParams.model_validate(valid_payload)
        assert params.uid == valid_payload["uid"]
        assert params.attributes == valid_payload["attributes"]

        # Invalid payload (incorrect metadata and missing uids)
        invalid_payload = {
            "email": "updated@example.com",
            "user_metadata": {"name": "Updated User"},
        }
        with pytest.raises(ValidationError) as excinfo:
            UpdateUserByIdParams.model_validate(invalid_payload)
        assert "uid" in str(excinfo.value)

    def test_delete_factor_conversion(self):
        """Test conversion of delete_factor JSON data"""
        # Valid payload
        valid_payload = {
            "user_id": "d0e8c69f-e0c3-4a1c-b6d6-9a6c756a6a4b",
            "id": "totp-factor-id-123",
        }
        params = DeleteFactorParams.model_validate(valid_payload)
        assert params.user_id == valid_payload["user_id"]
        assert params.id == valid_payload["id"]

        # Invalid payload (missing user_id)
        invalid_payload = {
            "id": "totp-factor-id-123",
        }
        with pytest.raises(ValidationError) as excinfo:
            DeleteFactorParams.model_validate(invalid_payload)
        assert "user_id" in str(excinfo.value)

        # Invalid payload (missing id)
        invalid_payload = {
            "user_id": "d0e8c69f-e0c3-4a1c-b6d6-9a6c756a6a4b",
        }
        with pytest.raises(ValidationError) as excinfo:
            DeleteFactorParams.model_validate(invalid_payload)
        assert "id" in str(excinfo.value)

    def test_param_models_mapping(self):
        """Test PARAM_MODELS mapping functionality"""
        # Test that all methods have the correct corresponding model
        method_model_pairs = [
            ("get_user_by_id", GetUserByIdParams),
            ("list_users", ListUsersParams),
            ("create_user", CreateUserParams),
            ("delete_user", DeleteUserParams),
            ("invite_user_by_email", InviteUserByEmailParams),
            ("generate_link", GenerateLinkParams),
            ("update_user_by_id", UpdateUserByIdParams),
            ("delete_factor", DeleteFactorParams),
        ]

        for method, expected_model in method_model_pairs:
            assert method in PARAM_MODELS
            assert PARAM_MODELS[method] == expected_model

        # Test actual validation of data through PARAM_MODELS mapping
        method = "create_user"
        model_class = PARAM_MODELS[method]

        valid_payload = {"email": "test@example.com", "password": "secure-password"}

        params = model_class.model_validate(valid_payload)
        assert params.email == valid_payload["email"]
        assert params.password == valid_payload["password"]
