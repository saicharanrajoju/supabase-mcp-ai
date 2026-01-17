def get_auth_admin_methods_spec() -> dict:
    """Returns a detailed specification of all Auth Admin methods."""
    return {
        "get_user_by_id": {
            "description": "Retrieve a user by their ID",
            "parameters": {"uid": {"type": "string", "description": "The user's UUID", "required": True}},
            "returns": {"type": "object", "description": "User object containing all user data"},
            "example": {
                "request": {"uid": "d0e8c69f-e0c3-4a1c-b6d6-9a6c756a6a4b"},
                "response": {
                    "id": "d0e8c69f-e0c3-4a1c-b6d6-9a6c756a6a4b",
                    "email": "user@example.com",
                    "phone": "",
                    "created_at": "2023-01-01T00:00:00Z",
                    "confirmed_at": "2023-01-01T00:00:00Z",
                    "last_sign_in_at": "2023-01-01T00:00:00Z",
                    "user_metadata": {"name": "John Doe"},
                    "app_metadata": {},
                },
            },
        },
        "list_users": {
            "description": "List all users with pagination",
            "parameters": {
                "page": {
                    "type": "integer",
                    "description": "Page number (starts at 1)",
                    "required": False,
                    "default": 1,
                },
                "per_page": {
                    "type": "integer",
                    "description": "Number of users per page",
                    "required": False,
                    "default": 50,
                },
            },
            "returns": {"type": "object", "description": "Paginated list of users with metadata"},
            "example": {
                "request": {"page": 1, "per_page": 10},
                "response": {
                    "users": [
                        {
                            "id": "d0e8c69f-e0c3-4a1c-b6d6-9a6c756a6a4b",
                            "email": "user@example.com",
                            "user_metadata": {"name": "John Doe"},
                        }
                    ],
                    "aud": "authenticated",
                    "total_count": 100,
                    "next_page": 2,
                },
            },
        },
        "create_user": {
            "description": "Create a new user. Does not send a confirmation email by default.",
            "parameters": {
                "email": {"type": "string", "description": "The user's email address"},
                "password": {"type": "string", "description": "The user's password"},
                "email_confirm": {
                    "type": "boolean",
                    "description": "Confirms the user's email address if set to true",
                    "default": False,
                },
                "phone": {"type": "string", "description": "The user's phone number with country code"},
                "phone_confirm": {
                    "type": "boolean",
                    "description": "Confirms the user's phone number if set to true",
                    "default": False,
                },
                "user_metadata": {
                    "type": "object",
                    "description": "A custom data object to store the user's metadata",
                },
                "app_metadata": {
                    "type": "object",
                    "description": "A custom data object to store the user's application specific metadata",
                },
                "role": {"type": "string", "description": "The role claim set in the user's access token JWT"},
                "ban_duration": {"type": "string", "description": "Determines how long a user is banned for"},
                "nonce": {
                    "type": "string",
                    "description": "The nonce (required for reauthentication if updating password)",
                },
            },
            "returns": {"type": "object", "description": "Created user object"},
            "example": {
                "request": {
                    "email": "new@example.com",
                    "password": "secure-password",
                    "email_confirm": True,
                    "user_metadata": {"name": "New User"},
                },
                "response": {
                    "id": "d0e8c69f-e0c3-4a1c-b6d6-9a6c756a6a4b",
                    "email": "new@example.com",
                    "email_confirmed_at": "2023-01-01T00:00:00Z",
                    "user_metadata": {"name": "New User"},
                },
            },
            "notes": "Either email or phone must be provided. Use invite_user_by_email() if you want to send an email invite.",
        },
        "delete_user": {
            "description": "Delete a user by their ID. Requires a service_role key.",
            "parameters": {
                "id": {"type": "string", "description": "The user's UUID", "required": True},
                "should_soft_delete": {
                    "type": "boolean",
                    "description": "If true, the user will be soft-deleted (preserving their data but disabling the account). Defaults to false.",
                    "required": False,
                    "default": False,
                },
            },
            "returns": {"type": "object", "description": "Success message"},
            "example": {
                "request": {"id": "d0e8c69f-e0c3-4a1c-b6d6-9a6c756a6a4b"},
                "response": {"message": "User deleted successfully"},
            },
            "notes": "This function should only be called on a server. Never expose your service_role key in the browser.",
        },
        "invite_user_by_email": {
            "description": "Sends an invite link to a user's email address. Typically used by administrators to invite users to join the application.",
            "parameters": {
                "email": {"type": "string", "description": "The email address of the user", "required": True},
                "options": {
                    "type": "object",
                    "description": "Optional settings for the invite",
                    "required": False,
                    "properties": {
                        "data": {
                            "type": "object",
                            "description": "A custom data object to store additional metadata about the user. Maps to auth.users.user_metadata",
                            "required": False,
                        },
                        "redirect_to": {
                            "type": "string",
                            "description": "The URL which will be appended to the email link. Once clicked the user will end up on this URL",
                            "required": False,
                        },
                    },
                },
            },
            "returns": {"type": "object", "description": "User object for the invited user"},
            "example": {
                "request": {
                    "email": "invite@example.com",
                    "options": {"data": {"name": "John Doe"}, "redirect_to": "https://example.com/welcome"},
                },
                "response": {
                    "id": "a1a1a1a1-a1a1-a1a1-a1a1-a1a1a1a1a1a1",
                    "email": "invite@example.com",
                    "role": "authenticated",
                    "email_confirmed_at": None,
                    "invited_at": "2023-01-01T00:00:00Z",
                },
            },
            "notes": "Note that PKCE is not supported when using invite_user_by_email. This is because the browser initiating the invite is often different from the browser accepting the invite.",
        },
        "generate_link": {
            "description": "Generate an email link for various authentication purposes. Handles user creation for signup, invite and magiclink types.",
            "parameters": {
                "type": {
                    "type": "string",
                    "description": "Link type: 'signup', 'invite', 'magiclink', 'recovery', 'email_change_current', 'email_change_new', 'phone_change'",
                    "required": True,
                    "enum": [
                        "signup",
                        "invite",
                        "magiclink",
                        "recovery",
                        "email_change_current",
                        "email_change_new",
                        "phone_change",
                    ],
                },
                "email": {"type": "string", "description": "User's email address", "required": True},
                "password": {
                    "type": "string",
                    "description": "User's password. Only required if type is signup",
                    "required": False,
                },
                "new_email": {
                    "type": "string",
                    "description": "New email address. Only required if type is email_change_current or email_change_new",
                    "required": False,
                },
                "options": {
                    "type": "object",
                    "description": "Additional options for the link",
                    "required": False,
                    "properties": {
                        "data": {
                            "type": "object",
                            "description": "Custom JSON object containing user metadata. Only accepted if type is signup, invite, or magiclink",
                            "required": False,
                        },
                        "redirect_to": {
                            "type": "string",
                            "description": "A redirect URL which will be appended to the generated email link",
                            "required": False,
                        },
                    },
                },
            },
            "returns": {"type": "object", "description": "Generated link details"},
            "example": {
                "request": {
                    "type": "signup",
                    "email": "user@example.com",
                    "password": "secure-password",
                    "options": {"data": {"name": "John Doe"}, "redirect_to": "https://example.com/welcome"},
                },
                "response": {
                    "action_link": "https://your-project.supabase.co/auth/v1/verify?token=...",
                    "email_otp": "123456",
                    "hashed_token": "...",
                    "redirect_to": "https://example.com/welcome",
                    "verification_type": "signup",
                },
            },
            "notes": "generate_link() only generates the email link for email_change_email if the Secure email change is enabled in your project's email auth provider settings.",
        },
        "update_user_by_id": {
            "description": "Update user attributes by ID. Requires a service_role key.",
            "parameters": {
                "uid": {"type": "string", "description": "The user's UUID", "required": True},
                "attributes": {
                    "type": "object",
                    "description": "The user attributes to update.",
                    "required": True,
                    "properties": {
                        "email": {"type": "string", "description": "The user's email"},
                        "phone": {"type": "string", "description": "The user's phone"},
                        "password": {"type": "string", "description": "The user's password"},
                        "email_confirm": {
                            "type": "boolean",
                            "description": "Confirms the user's email address if set to true",
                        },
                        "phone_confirm": {
                            "type": "boolean",
                            "description": "Confirms the user's phone number if set to true",
                        },
                        "user_metadata": {
                            "type": "object",
                            "description": "A custom data object to store the user's metadata.",
                        },
                        "app_metadata": {
                            "type": "object",
                            "description": "A custom data object to store the user's application specific metadata.",
                        },
                        "role": {
                            "type": "string",
                            "description": "The role claim set in the user's access token JWT",
                        },
                        "ban_duration": {
                            "type": "string",
                            "description": "Determines how long a user is banned for",
                        },
                        "nonce": {
                            "type": "string",
                            "description": "The nonce sent for reauthentication if the user's password is to be updated",
                        },
                    },
                },
            },
            "returns": {"type": "object", "description": "Updated user object"},
            "example": {
                "request": {
                    "uid": "d0e8c69f-e0c3-4a1c-b6d6-9a6c756a6a4b",
                    "attributes": {"email": "updated@example.com", "user_metadata": {"name": "Updated Name"}},
                },
                "response": {
                    "id": "d0e8c69f-e0c3-4a1c-b6d6-9a6c756a6a4b",
                    "email": "updated@example.com",
                    "user_metadata": {"name": "Updated Name"},
                },
            },
            "notes": "This function should only be called on a server. Never expose your service_role key in the browser.",
        },
        "delete_factor": {
            "description": "Deletes a factor on a user. This will log the user out of all active sessions if the deleted factor was verified.",
            "parameters": {
                "user_id": {
                    "type": "string",
                    "description": "ID of the user whose factor is being deleted",
                    "required": True,
                },
                "id": {"type": "string", "description": "ID of the MFA factor to delete", "required": True},
            },
            "returns": {"type": "object", "description": "Success message"},
            "example": {
                "request": {"user_id": "d0e8c69f-e0c3-4a1c-b6d6-9a6c756a6a4b", "id": "totp-factor-id-123"},
                "response": {"message": "Factor deleted successfully"},
            },
            "notes": "This will log the user out of all active sessions if the deleted factor was verified.",
        },
    }
