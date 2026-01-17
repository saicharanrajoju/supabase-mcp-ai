
import os
import datetime
from langchain_anthropic import ChatAnthropic
from langchain_core.prompts import PromptTemplate
from src.logger import logger
from src.services.database.sql.validator import SQLValidator
from src.services.database.sql.models import ValidatedStatement, SQLQueryCommand
from src.ai_schema_search import SchemaSearcher

class MigrationGenerator:
    def __init__(self, schema_searcher: SchemaSearcher):
        self.schema_searcher = schema_searcher
        self.validate = SQLValidator()
        api_key = os.getenv("ANTHROPIC_API_KEY")
        self.llm = ChatAnthropic(
            model="claude-sonnet-4-20250514", 
            temperature=0, 
            api_key=api_key
        )
        self.migration_log_path = "migrations.log"

    async def create_migration_from_nl(self, description: str) -> str:
        """
        Generate a SQL migration script from natural language description.
        """
        logger.info(f"Generating migration for: {description}")

        # 1. Get Schema Context
        # Search for potentially relevant tables to include in context
        relevant_items = self.schema_searcher.semantic_search_schema(description, top_k=20)
        
        # Group by table to provide full table context if possible, 
        # or just list found columns.
        schema_context = "Relevant Schema Information:\n"
        for item in relevant_items:
            schema_context += f"- Table: {item['table_name']}, Column: {item['column_name']} ({item['data_type']})\n"

        # 2. Prompt LLM
        prompt_template = PromptTemplate.from_template("""
        You are an expert PostgreSQL database administrator.
        Generate a SQL migration script based on the following request.
        
        Request: {description}
        
        Context (Existing Schema):
        {context}
        
        Rules:
        1. Return ONLY the SQL code. No markdown, no explanations.
        2. Use compatible PostgreSQL syntax.
        3. Prefer ALTER TABLE or CREATE TABLE.
        4. Do NOT use DROP TABLE or DELETE without explicit user instruction in the request.
        
        SQL:
        """)
        
        response = await self.llm.ainvoke({
            "description": description,
            "context": schema_context
        })
        
        sql = response.content.strip()
        # Remove markdown code blocks if present
        if sql.startswith("```sql"):
            sql = sql.replace("```sql", "").replace("```", "")
        elif sql.startswith("```"):
            sql = sql.replace("```", "")
        
        sql = sql.strip()

        # 3. Validate SQL
        try:
            validation_result = self.validate.validate_query(sql)
            
            # 4. Safety Checks
            for stmt in validation_result.statements:
                if stmt.command in [SQLQueryCommand.DROP, SQLQueryCommand.DELETE, SQLQueryCommand.TRUNCATE]:
                    # Check if description explicitly asks for it? 
                    # For now, we flag it as potentially unsafe if strict.
                    # Prompt requirement: "Add safety checks (no DROP, DELETE without confirmation)"
                    # We will log a warning and maybe prepend a comment or raise error?
                    # I'll raise an error to enforce "confirmation" (user must retry or use a flag, but strict signature is (str)->str).
                    # I will fail it.
                    if "force" not in description.lower():
                        raise ValueError(f"Unsafe operation detected ({stmt.command}). request 'force' in description to override.")

        except Exception as e:
            logger.error(f"Migration validation failed: {e}")
            raise ValueError(f"Generated SQL failed validation: {e}")

        # 5. Log
        self._log_migration(description, sql)
        
        return sql

    def _log_migration(self, description: str, sql: str):
        timestamp = datetime.datetime.now().isoformat()
        entry = f"[{timestamp}] Request: {description}\nSQL:\n{sql}\n{'-'*40}\n"
        try:
            with open(self.migration_log_path, "a") as f:
                f.write(entry)
        except Exception as e:
            logger.error(f"Failed to write to migration log: {e}")
