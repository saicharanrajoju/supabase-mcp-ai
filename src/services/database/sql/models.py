from enum import Enum

from pydantic import BaseModel, Field

from src.services.safety.models import OperationRiskLevel


class SQLQueryCategory(str, Enum):
    """Category of the SQL query tracked by the SQL validator"""

    DQL = "DQL"  # Data Query Language (SELECT)
    DML = "DML"  # Data Manipulation Language (INSERT, UPDATE, DELETE)
    DDL = "DDL"  # Data Definition Language (CREATE, ALTER, DROP)
    TCL = "TCL"  # Transaction Control Language (BEGIN, COMMIT, ROLLBACK)
    DCL = "DCL"  # Data Control Language (GRANT, REVOKE)
    POSTGRES_SPECIFIC = "POSTGRES_SPECIFIC"  # PostgreSQL-specific commands
    OTHER = "OTHER"  # Other commands not fitting into the categories above


class SQLQueryCommand(str, Enum):
    """Command of the SQL query tracked by the SQL validator"""

    # DQL Commands
    SELECT = "SELECT"

    # DML Commands
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    MERGE = "MERGE"

    # DDL Commands
    CREATE = "CREATE"
    ALTER = "ALTER"
    DROP = "DROP"
    TRUNCATE = "TRUNCATE"
    COMMENT = "COMMENT"
    RENAME = "RENAME"

    # DCL Commands
    GRANT = "GRANT"
    REVOKE = "REVOKE"

    # TCL Commands (for tracking, not query types)
    BEGIN = "BEGIN"
    COMMIT = "COMMIT"
    ROLLBACK = "ROLLBACK"
    SAVEPOINT = "SAVEPOINT"

    # PostgreSQL-specific Commands
    VACUUM = "VACUUM"
    ANALYZE = "ANALYZE"
    EXPLAIN = "EXPLAIN"
    COPY = "COPY"
    LISTEN = "LISTEN"
    NOTIFY = "NOTIFY"
    PREPARE = "PREPARE"
    EXECUTE = "EXECUTE"
    DEALLOCATE = "DEALLOCATE"

    # Other/Unknown
    UNKNOWN = "UNKNOWN"


class ValidatedStatement(BaseModel):
    """Result of the query validation for a single SQL statement."""

    category: SQLQueryCategory = Field(
        ..., description="The category of SQL statement (DQL, DML, DDL, etc.) derived from pglast parse tree"
    )
    risk_level: OperationRiskLevel = Field(
        ..., description="The risk level associated with this statement based on category and command"
    )
    command: SQLQueryCommand = Field(
        ..., description="The specific SQL command (SELECT, INSERT, CREATE, etc.) extracted from parse tree"
    )
    object_type: str | None = Field(
        None, description="The type of object being operated on (TABLE, INDEX, etc.) when available"
    )
    schema_name: str | None = Field(None, description="The schema name for the objects in the statement when available")
    needs_migration: bool = Field(
        ..., description="Whether this statement requires a migration based on statement type and safety rules"
    )
    query: str | None = Field(None, description="The actual SQL text for this statement extracted from original query")


class QueryValidationResults(BaseModel):
    """Result of the batch validation for one or more SQL statements."""

    statements: list[ValidatedStatement] = Field(
        default_factory=list, description="List of validated statements from the query built during validation"
    )
    highest_risk_level: OperationRiskLevel = Field(
        default=OperationRiskLevel.LOW, description="The highest risk level among all statements in the batch"
    )
    has_transaction_control: bool = Field(
        default=False, description="Whether the query contains transaction control statements (BEGIN, COMMIT, etc.)"
    )
    original_query: str = Field(..., description="The original SQL query text as provided by the user")

    def needs_migration(self) -> bool:
        """Check if any statement in the batch needs migration."""
        return any(stmt.needs_migration for stmt in self.statements)
