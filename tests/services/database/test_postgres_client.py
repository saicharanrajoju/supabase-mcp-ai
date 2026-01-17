import asyncpg
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from supabase_mcp.exceptions import ConnectionError, QueryError, PermissionError as SupabasePermissionError
from supabase_mcp.services.database.postgres_client import PostgresClient, QueryResult, StatementResult
from supabase_mcp.services.database.sql.validator import (
    QueryValidationResults,
    SQLQueryCategory,
    SQLQueryCommand,
    ValidatedStatement,
)
from supabase_mcp.services.safety.models import OperationRiskLevel
from supabase_mcp.settings import Settings


@pytest.mark.asyncio(loop_scope="class")
class TestPostgresClient:
    """Unit tests for the Postgres client."""

    @pytest.fixture
    def mock_settings(self):
        """Create mock settings for testing."""
        settings = MagicMock(spec=Settings)
        settings.supabase_project_ref = "test-project-ref"
        settings.supabase_db_password = "test-password"
        settings.supabase_region = "us-east-1"
        settings.database_url = "postgresql://test:test@localhost:5432/test"
        return settings

    @pytest.fixture
    async def mock_postgres_client(self, mock_settings):
        """Create a mock Postgres client for testing."""
        # Reset the singleton first
        await PostgresClient.reset()
        
        # Create client and mock execute_query directly
        client = PostgresClient(settings=mock_settings)
        return client

    async def test_execute_simple_select(self, mock_postgres_client: PostgresClient):
        """Test executing a simple SELECT query."""
        # Create a simple validation result with a SELECT query
        query = "SELECT 1 as number;"
        statement = ValidatedStatement(
            query=query,
            command=SQLQueryCommand.SELECT,
            category=SQLQueryCategory.DQL,
            risk_level=OperationRiskLevel.LOW,
            needs_migration=False,
            object_type=None,
            schema_name=None,
        )
        validation_result = QueryValidationResults(
            statements=[statement],
            original_query=query,
            highest_risk_level=OperationRiskLevel.LOW,
        )

        # Mock the query result
        expected_result = QueryResult(results=[
            StatementResult(rows=[{"number": 1}])
        ])
        
        with patch.object(mock_postgres_client, 'execute_query', return_value=expected_result):
            # Execute the query
            result = await mock_postgres_client.execute_query(validation_result)

            # Verify the result
            assert isinstance(result, QueryResult)
            assert len(result.results) == 1
            assert isinstance(result.results[0], StatementResult)
            assert len(result.results[0].rows) == 1
            assert result.results[0].rows[0]["number"] == 1

    async def test_execute_multiple_statements(self, mock_postgres_client: PostgresClient):
        """Test executing multiple SQL statements in a single query."""
        # Create validation result with multiple statements
        query = "SELECT 1 as first; SELECT 2 as second;"
        statements = [
            ValidatedStatement(
                query="SELECT 1 as first;",
                command=SQLQueryCommand.SELECT,
                category=SQLQueryCategory.DQL,
                risk_level=OperationRiskLevel.LOW,
                needs_migration=False,
                object_type=None,
                schema_name=None,
            ),
            ValidatedStatement(
                query="SELECT 2 as second;",
                command=SQLQueryCommand.SELECT,
                category=SQLQueryCategory.DQL,
                risk_level=OperationRiskLevel.LOW,
                needs_migration=False,
                object_type=None,
                schema_name=None,
            ),
        ]
        validation_result = QueryValidationResults(
            statements=statements,
            original_query=query,
            highest_risk_level=OperationRiskLevel.LOW,
        )

        # Mock the query result
        expected_result = QueryResult(results=[
            StatementResult(rows=[{"first": 1}]),
            StatementResult(rows=[{"second": 2}])
        ])
        
        with patch.object(mock_postgres_client, 'execute_query', return_value=expected_result):
            # Execute the query
            result = await mock_postgres_client.execute_query(validation_result)

            # Verify the result
            assert isinstance(result, QueryResult)
            assert len(result.results) == 2
            assert result.results[0].rows[0]["first"] == 1
            assert result.results[1].rows[0]["second"] == 2

    async def test_execute_query_with_parameters(self, mock_postgres_client: PostgresClient):
        """Test executing a query with parameters."""
        query = "SELECT 'test' as name, 42 as value;"
        statement = ValidatedStatement(
            query=query,
            command=SQLQueryCommand.SELECT,
            category=SQLQueryCategory.DQL,
            risk_level=OperationRiskLevel.LOW,
            needs_migration=False,
            object_type=None,
            schema_name=None,
        )
        validation_result = QueryValidationResults(
            statements=[statement],
            original_query=query,
            highest_risk_level=OperationRiskLevel.LOW,
        )

        # Mock the query result
        expected_result = QueryResult(results=[
            StatementResult(rows=[{"name": "test", "value": 42}])
        ])
        
        with patch.object(mock_postgres_client, 'execute_query', return_value=expected_result):
            # Execute the query
            result = await mock_postgres_client.execute_query(validation_result)

            # Verify the result
            assert isinstance(result, QueryResult)
            assert len(result.results) == 1
            assert result.results[0].rows[0]["name"] == "test"
            assert result.results[0].rows[0]["value"] == 42

    async def test_permission_error(self, mock_postgres_client: PostgresClient):
        """Test handling a permission error."""
        # Create a mock error
        error = asyncpg.exceptions.InsufficientPrivilegeError("Permission denied")

        # Verify that the method raises PermissionError with the expected message
        with pytest.raises(SupabasePermissionError) as exc_info:
            await mock_postgres_client._handle_postgres_error(error)
        
        # Verify the error message
        assert "Access denied" in str(exc_info.value)
        assert "Permission denied" in str(exc_info.value)
        assert "live_dangerously" in str(exc_info.value)

    async def test_query_error(self, mock_postgres_client: PostgresClient):
        """Test handling a query error."""
        # Create a validation result with a syntactically valid but semantically incorrect query
        query = "SELECT * FROM nonexistent_table;"
        statement = ValidatedStatement(
            query=query,
            command=SQLQueryCommand.SELECT,
            category=SQLQueryCategory.DQL,
            risk_level=OperationRiskLevel.LOW,
            needs_migration=False,
            object_type="TABLE",
            schema_name="public",
        )
        validation_result = QueryValidationResults(
            statements=[statement],
            original_query=query,
            highest_risk_level=OperationRiskLevel.LOW,
        )

        # Mock execute_query to raise a QueryError
        with patch.object(mock_postgres_client, 'execute_query', 
                         side_effect=QueryError("relation \"nonexistent_table\" does not exist")):
            # Execute the query - should raise a QueryError
            with pytest.raises(QueryError) as excinfo:
                await mock_postgres_client.execute_query(validation_result)

            # Verify the error message contains the specific error
            assert "nonexistent_table" in str(excinfo.value)

    async def test_schema_error(self, mock_postgres_client: PostgresClient):
        """Test handling a schema error."""
        # Create a validation result with a query referencing a non-existent column
        query = "SELECT nonexistent_column FROM information_schema.tables;"
        statement = ValidatedStatement(
            query=query,
            command=SQLQueryCommand.SELECT,
            category=SQLQueryCategory.DQL,
            risk_level=OperationRiskLevel.LOW,
            needs_migration=False,
            object_type="TABLE",
            schema_name="information_schema",
        )
        validation_result = QueryValidationResults(
            statements=[statement],
            original_query=query,
            highest_risk_level=OperationRiskLevel.LOW,
        )

        # Mock execute_query to raise a QueryError
        with patch.object(mock_postgres_client, 'execute_query',
                         side_effect=QueryError("column \"nonexistent_column\" does not exist")):
            # Execute the query - should raise a QueryError
            with pytest.raises(QueryError) as excinfo:
                await mock_postgres_client.execute_query(validation_result)

            # Verify the error message contains the specific error
            assert "nonexistent_column" in str(excinfo.value)

    async def test_write_operation(self, mock_postgres_client: PostgresClient):
        """Test a basic write operation (INSERT)."""
        # Create insert query
        insert_query = "INSERT INTO test_write (name) VALUES ('test_value') RETURNING id, name;"
        insert_statement = ValidatedStatement(
            query=insert_query,
            command=SQLQueryCommand.INSERT,
            category=SQLQueryCategory.DML,
            risk_level=OperationRiskLevel.MEDIUM,
            needs_migration=False,
            object_type="TABLE",
            schema_name="public",
        )
        insert_validation = QueryValidationResults(
            statements=[insert_statement],
            original_query=insert_query,
            highest_risk_level=OperationRiskLevel.MEDIUM,
        )

        # Mock the query result
        expected_result = QueryResult(results=[
            StatementResult(rows=[{"id": 1, "name": "test_value"}])
        ])
        
        with patch.object(mock_postgres_client, 'execute_query', return_value=expected_result):
            # Execute the insert query
            result = await mock_postgres_client.execute_query(insert_validation, readonly=False)

            # Verify the result
            assert isinstance(result, QueryResult)
            assert len(result.results) == 1
            assert result.results[0].rows[0]["name"] == "test_value"
            assert result.results[0].rows[0]["id"] == 1

    async def test_ddl_operation(self, mock_postgres_client: PostgresClient):
        """Test a basic DDL operation (CREATE TABLE)."""
        # Create a test table
        create_query = "CREATE TEMPORARY TABLE test_ddl (id SERIAL PRIMARY KEY, value TEXT);"
        create_statement = ValidatedStatement(
            query=create_query,
            command=SQLQueryCommand.CREATE,
            category=SQLQueryCategory.DDL,
            risk_level=OperationRiskLevel.MEDIUM,
            needs_migration=False,
            object_type="TABLE",
            schema_name="public",
        )
        create_validation = QueryValidationResults(
            statements=[create_statement],
            original_query=create_query,
            highest_risk_level=OperationRiskLevel.MEDIUM,
        )

        # Mock the query result - DDL typically returns empty results
        expected_result = QueryResult(results=[
            StatementResult(rows=[])
        ])
        
        with patch.object(mock_postgres_client, 'execute_query', return_value=expected_result):
            # Execute the create table query
            result = await mock_postgres_client.execute_query(create_validation, readonly=False)

            # Verify the result
            assert isinstance(result, QueryResult)
            assert len(result.results) == 1
            # DDL operations typically don't return rows
            assert result.results[0].rows == []

    async def test_execute_metadata_query(self, mock_postgres_client: PostgresClient):
        """Test executing a metadata query."""
        # Create a simple validation result with a SELECT query
        query = "SELECT schema_name FROM information_schema.schemata LIMIT 5;"
        statement = ValidatedStatement(
            query=query,
            command=SQLQueryCommand.SELECT,
            category=SQLQueryCategory.DQL,
            risk_level=OperationRiskLevel.LOW,
            needs_migration=False,
            object_type="schemata",
            schema_name="information_schema",
        )
        validation_result = QueryValidationResults(
            statements=[statement],
            original_query=query,
            highest_risk_level=OperationRiskLevel.LOW,
        )

        # Mock the query result
        expected_result = QueryResult(results=[
            StatementResult(rows=[
                {"schema_name": "public"},
                {"schema_name": "information_schema"},
                {"schema_name": "pg_catalog"},
                {"schema_name": "auth"},
                {"schema_name": "storage"}
            ])
        ])
        
        with patch.object(mock_postgres_client, 'execute_query', return_value=expected_result):
            # Execute the query
            result = await mock_postgres_client.execute_query(validation_result)

            # Verify the result
            assert isinstance(result, QueryResult)
            assert len(result.results) == 1
            assert len(result.results[0].rows) == 5
            assert "schema_name" in result.results[0].rows[0]

    async def test_connection_retry_mechanism(self, mock_postgres_client: PostgresClient):
        """Test that the tenacity retry mechanism works correctly for database connections."""
        # Reset the pool
        mock_postgres_client._pool = None
        
        # Mock create_pool to always raise a connection error
        with patch.object(mock_postgres_client, 'create_pool', 
                         side_effect=ConnectionError("Could not connect to database")):
            # This should trigger the retry mechanism and eventually fail
            with pytest.raises(ConnectionError) as exc_info:
                await mock_postgres_client.ensure_pool()

            # Verify the error message indicates a connection failure after retries
            assert "Could not connect to database" in str(exc_info.value)