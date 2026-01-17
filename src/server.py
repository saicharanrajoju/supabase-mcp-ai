
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
import os
import json

from mcp.server.fastmcp import FastMCP

from src.core.container import ServicesContainer
from src.logger import logger
from src.settings import settings
from src.tools.registry import ToolRegistry

# AI Imports
from src.ai_schema_search import SchemaSearcher
from src.langchain_agents import LangChainAgent
from src.nl_migrations import MigrationGenerator

# Global AI instances
schema_searcher: SchemaSearcher | None = None
langchain_agent: LangChainAgent | None = None
migration_generator: MigrationGenerator | None = None

# Create lifespan for the MCP server
@asynccontextmanager
async def lifespan(app: FastMCP) -> AsyncGenerator[FastMCP, None]:
    global schema_searcher, langchain_agent, migration_generator
    try:
        logger.info("Initializing services")

        # Initialize services
        services_container = ServicesContainer.get_instance()
        services_container.initialize_services(settings)

        # Initialize AI Modules
        logger.info("Initializing AI capabilities...")
        
        # 1. AI Schema Search
        schema_searcher = SchemaSearcher(services_container.postgres_client)
        # We ensure pool is ready before fetching schema
        await services_container.postgres_client.ensure_pool()
        await schema_searcher.initialize()

        # 2. LangChain Agent
        langchain_agent = LangChainAgent(
            query_manager=services_container.query_manager,
            schema_searcher=schema_searcher
        )

        # 3. Migration Generator
        migration_generator = MigrationGenerator(schema_searcher)

        logger.info("AI modules initialized.")

        # Register existing tools
        mcp = ToolRegistry(mcp=app, services_container=services_container).register_tools()
        yield mcp
    finally:
        logger.info("Shutting down services")
        services_container = ServicesContainer.get_instance()
        await services_container.shutdown_services()
        # Force kill the entire process
        os._exit(0)


# Create mcp instance
mcp = FastMCP("supabase", lifespan=lifespan)

# --- AI Tools ---

@mcp.tool()
async def ai_query(question: str) -> str:
    """
    Ask a natural language question about the database data or structure.
    The AI agent can query the database, search the schema, and explain results.
    """
    if not langchain_agent:
        return "AI Agent not initialized."
    return await langchain_agent.natural_language_query(question)

@mcp.tool()
async def search_schema(query: str) -> str:
    """
    Perform a semantic search on the database schema.
    Returns relevant tables, columns, and descriptions based on embedding similarity.
    """
    if not schema_searcher:
        return "Schema Searcher not initialized."
    
    results = schema_searcher.semantic_search_schema(query)
    if not results:
        return "No relevant schema items found."
    
    # Format results
    output = ["Found relevant schema items:"]
    for item in results:
        score = item.get('relevance_score', 0)
        output.append(f"- {item['table_name']}.{item['column_name']} ({item['data_type']}) [Score: {score:.2f}]")
        if item.get('column_description'):
            output.append(f"  Description: {item['column_description']}")
            
    return "\n".join(output)

@mcp.tool()
async def generate_migration(description: str) -> str:
    """
    Generate a SQL migration script from a natural language description.
    Example: 'Add a phone_number column to users table'
    """
    if not migration_generator:
        return "Migration Generator not initialized."
    
    try:
        sql = await migration_generator.create_migration_from_nl(description)
        return f"Generated Migration:\n\n{sql}"
    except Exception as e:
        return f"failed to generate migration: {str(e)}"


def run_server() -> None:
    logger.info("Starting Supabase MCP server")
    mcp.run()

def run_inspector() -> None:
    """Inspector mode"""
    logger.info("Starting Supabase MCP server inspector")
    from mcp.cli.cli import dev
    return dev(__file__)

if __name__ == "__main__":
    logger.info("Starting Supabase MCP server")
    run_server()
