from typing import Any, Literal

from pydantic import BaseModel, model_validator


class GetUserByIdParams(BaseModel):
    uid: str


class ListUsersParams(BaseModel):
    page: int | None = 1
    per_page: int | None = 50


class UserMetadata(BaseModel):
    email: str | None = None
    email_verified: bool | None = None
    phone_verified: bool | None = None
    sub: str | None = None


class AdminUserAttributes(BaseModel):
    email: str | None = None
    password: str | None = None
    email_confirm: bool | None = False
    phone: str | None = None
    phone_confirm: bool | None = False
    user_metadata: UserMetadata | None = None
    app_metadata: dict[str, Any] | None = None
    role: str | None = None
    ban_duration: str | None = None
    nonce: str | None = None


class CreateUserParams(AdminUserAttributes):
    pass

    @model_validator(mode="after")
    def check_email_or_phone(self) -> "CreateUserParams":
        if not self.email and not self.phone:
            raise ValueError("Either email or phone must be provided")
        return self


class DeleteUserParams(BaseModel):
    id: str
    should_soft_delete: bool | None = False


class InviteUserByEmailParams(BaseModel):
    email: str
    options: dict[str, Any] | None = None


class GenerateLinkParams(BaseModel):
    type: Literal[
        "signup", "invite", "magiclink", "recovery", "email_change_current", "email_change_new", "phone_change"
    ]
    email: str
    password: str | None = None
    new_email: str | None = None
    options: dict[str, Any] | None = None

    @model_validator(mode="after")
    def validate_required_fields(self) -> "GenerateLinkParams":
        # Check password for signup
        if self.type == "signup" and not self.password:
            raise ValueError("Password is required for signup links")

        # Check new_email for email change
        if self.type in ["email_change_current", "email_change_new"] and not self.new_email:
            raise ValueError("new_email is required for email change links")

        return self


class UpdateUserByIdParams(BaseModel):
    uid: str
    attributes: AdminUserAttributes


class DeleteFactorParams(BaseModel):
    id: str
    user_id: str


# Map method names to their parameter models
PARAM_MODELS = {
    "get_user_by_id": GetUserByIdParams,
    "list_users": ListUsersParams,
    "create_user": CreateUserParams,
    "delete_user": DeleteUserParams,
    "invite_user_by_email": InviteUserByEmailParams,
    "generate_link": GenerateLinkParams,
    "update_user_by_id": UpdateUserByIdParams,
    "delete_factor": DeleteFactorParams,
}
