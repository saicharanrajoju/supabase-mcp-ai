from src.exceptions import OperationNotAllowedError
from src.logger import logger
from src.services.database.migration_manager import MigrationManager
from src.services.database.postgres_client import PostgresClient, QueryResult
from src.services.database.sql.loader import SQLLoader
from src.services.database.sql.models import QueryValidationResults
from src.services.database.sql.validator import SQLValidator
from src.services.safety.models import ClientType, SafetyMode
from src.services.safety.safety_manager import SafetyManager


class QueryManager:
    """
    Manages SQL query execution with validation and migration handling.

    This class is responsible for:
    1. Validating SQL queries for safety
    2. Executing queries through the database client
    3. Managing migrations for queries that require them
    4. Loading SQL queries from files

    It acts as a central point for all SQL operations, ensuring consistent
    validation and execution patterns.
    """

    def __init__(
        self,
        postgres_client: PostgresClient,
        safety_manager: SafetyManager,
        sql_validator: SQLValidator | None = None,
        migration_manager: MigrationManager | None = None,
        sql_loader: SQLLoader | None = None,
    ):
        """
        Initialize the QueryManager.

        Args:
            postgres_client: The database client to use for executing queries
            safety_manager: The safety manager to use for validating operations
            sql_validator: Optional SQL validator to use
            migration_manager: Optional migration manager to use
            sql_loader: Optional SQL loader to use
        """
        self.db_client = postgres_client
        self.safety_manager = safety_manager
        self.validator = sql_validator or SQLValidator()
        self.sql_loader = sql_loader or SQLLoader()
        self.migration_manager = migration_manager or MigrationManager(loader=self.sql_loader)

    def check_readonly(self) -> bool:
        """Returns true if current safety mode is SAFE."""
        result = self.safety_manager.get_safety_mode(ClientType.DATABASE) == SafetyMode.SAFE
        logger.debug(f"Check readonly result: {result}")
        return result

    async def handle_query(self, query: str, has_confirmation: bool = False, migration_name: str = "") -> QueryResult:
        """
        Handle a SQL query with validation and potential migration. Uses migration name, if provided.

        This method:
        1. Validates the query for safety
        2. Checks if the query requires migration
        3. Handles migration if needed
        4. Executes the query

        Args:
            query: SQL query to execute
            params: Query parameters
            has_confirmation: Whether the operation has been confirmed by the user

        Returns:
            QueryResult: The result of the query execution

        Raises:
            OperationNotAllowedError: If the query is not allowed in the current safety mode
            ConfirmationRequiredError: If the query requires confirmation and has_confirmation is False
        """
        # 1. Run through the validator
        validated_query = self.validator.validate_query(query)

        # 2. Ensure execution is allowed
        self.safety_manager.validate_operation(ClientType.DATABASE, validated_query, has_confirmation)
        logger.debug(f"Operation with risk level {validated_query.highest_risk_level} validated successfully")

        # 3. Handle migration if needed
        await self.handle_migration(validated_query, query, migration_name)

        # 4. Execute the query
        return await self.handle_query_execution(validated_query)

    async def handle_query_execution(self, validated_query: QueryValidationResults) -> QueryResult:
        """
        Handle query execution with validation and potential migration.

        This method:
        1. Checks the readonly mode
        2. Executes the query
        3. Returns the result

        Args:
            validated_query: The validation result
            query: The original query

        Returns:
            QueryResult: The result of the query execution
        """
        readonly = self.check_readonly()
        result = await self.db_client.execute_query(validated_query, readonly)
        logger.debug(f"Query result: {result}")
        return result

    async def handle_migration(
        self, validation_result: QueryValidationResults, original_query: str, migration_name: str = ""
    ) -> None:
        """
        Handle migration for a query that requires it.

        Args:
            validation_result: The validation result
            query: The original query
            migration_name: Migration name to use, if provided
        """
        # 1. Check if migration is needed
        if not validation_result.needs_migration():
            logger.debug("No migration needed for this query")
            return

        # 2. Prepare migration query
        migration_query, name = self.migration_manager.prepare_migration_query(
            validation_result, original_query, migration_name
        )
        logger.debug("Migration query prepared")

        # 3. Execute migration query
        try:
            # First, ensure the migration schema exists
            await self.init_migration_schema()

            # Then execute the migration query
            migration_validation = self.validator.validate_query(migration_query)
            await self.db_client.execute_query(migration_validation, readonly=False)
            logger.info(f"Migration '{name}' executed successfully")
        except Exception as e:
            logger.debug(f"Migration failure details: {str(e)}")
            # We don't want to fail the main query if migration fails
            # Just log the error and continue
            logger.warning(f"Failed to record migration '{name}': {e}")

    async def init_migration_schema(self) -> None:
        """Initialize the migrations schema and table if they don't exist."""
        try:
            # Get the initialization query
            init_query = self.sql_loader.get_init_migrations_query()

            # Validate and execute it
            init_validation = self.validator.validate_query(init_query)
            await self.db_client.execute_query(init_validation, readonly=False)
            logger.debug("Migrations schema initialized successfully")
        except Exception as e:
            logger.warning(f"Failed to initialize migrations schema: {e}")

    async def handle_confirmation(self, confirmation_id: str) -> QueryResult:
        """
        Handle a confirmed operation using its confirmation ID.

        This method retrieves the stored operation and passes it to handle_query.

        Args:
            confirmation_id: The unique ID of the confirmation to process

        Returns:
            QueryResult: The result of the query execution
        """
        # Get the stored operation
        operation = self.safety_manager.get_stored_operation(confirmation_id)
        if not operation:
            raise OperationNotAllowedError(f"Invalid or expired confirmation ID: {confirmation_id}")

        # Get the query from the operation
        query = operation.original_query
        logger.debug(f"Processing confirmed operation with ID {confirmation_id}")

        # Call handle_query with the query and has_confirmation=True
        return await self.handle_query(query, has_confirmation=True)

    def get_schemas_query(self) -> str:
        """Get a query to list all schemas."""
        return self.sql_loader.get_schemas_query()

    def get_tables_query(self, schema_name: str) -> str:
        """Get a query to list all tables in a schema."""
        return self.sql_loader.get_tables_query(schema_name)

    def get_table_schema_query(self, schema_name: str, table: str) -> str:
        """Get a query to get the schema of a table."""
        return self.sql_loader.get_table_schema_query(schema_name, table)

    def get_migrations_query(
        self, limit: int = 50, offset: int = 0, name_pattern: str = "", include_full_queries: bool = False
    ) -> str:
        """Get a query to list migrations."""
        return self.sql_loader.get_migrations_query(
            limit=limit, offset=offset, name_pattern=name_pattern, include_full_queries=include_full_queries
        )
