
import os
import asyncio
from typing import List, Dict, Optional, Any
from langchain_anthropic import ChatAnthropic
from langchain.agents import AgentExecutor, create_react_agent, Tool
from langchain_core.prompts import PromptTemplate
from langchain.memory import ConversationBufferMemory

from src.logger import logger
from src.services.database.query_manager import QueryManager
from src.ai_schema_search import SchemaSearcher

class LangChainAgent:
    def __init__(self, query_manager: QueryManager, schema_searcher: SchemaSearcher):
        self.query_manager = query_manager
        self.schema_searcher = schema_searcher
        self.memory = ConversationBufferMemory(memory_key="chat_history", return_messages=True)
        self.agent_executor = self._initialize_agent()

    def _initialize_agent(self) -> AgentExecutor:
        # Define Tools with async implementations
        tools = [
            Tool(
                name="execute_sql",
                func=self._execute_sql_sync_placeholder, # Sync placeholder (should not be called in async run)
                coroutine=self._execute_sql_async,       # Async implementation
                description="Execute a SQL query against the database. Use this to run SELECT, INSERT, UPDATE, DELETE statements."
            ),
            Tool(
                name="search_schema",
                func=self._search_schema_sync,
                description="Search the database schema semantically. Input should be a natural language description."
            ),
            Tool(
                name="create_table",
                func=self._execute_sql_sync_placeholder,
                coroutine=self._create_table_async,
                description="Create a new table in the database. Input should be the full CREATE TABLE SQL statement."
            ),
            Tool(
                name="alter_table",
                func=self._execute_sql_sync_placeholder,
                coroutine=self._alter_table_async,
                description="Modify an existing table structure. Input should be the full ALTER TABLE SQL statement."
            )
        ]

        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            logger.warning("ANTHROPIC_API_KEY not found in environment")
        
        llm = ChatAnthropic(
            model="claude-sonnet-4-20250514",
            temperature=0,
            api_key=api_key
        )

        template = """Answer the following questions as best you can. You have access to the following tools:

{tools}

Use the following format:

Question: the input question you must answer
Thought: you should always think about what to do
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question

Begin!

Question: {input}
Thought:{agent_scratchpad}"""

        prompt = PromptTemplate.from_template(template)
        agent = create_react_agent(llm, tools, prompt)

        return AgentExecutor(
            agent=agent, 
            tools=tools, 
            verbose=True, 
            handle_parsing_errors=True,
            memory=self.memory
        )

    def _execute_sql_sync_placeholder(self, query: str) -> str:
        return "Error: Async tool called synchronously."

    async def _execute_sql_async(self, query: str) -> str:
        try:
            # query_manager.handle_query returns a QueryResult object
            result = await self.query_manager.handle_query(query)
            # Format QueryResult to string
            rows = []
            for stmt in result.results:
                rows.extend(stmt.rows)
            return f"Query executed successfully. Results: {rows}"
        except Exception as e:
            logger.error(f"SQL error: {e}")
            return f"Error executing SQL: {str(e)}"

    def _search_schema_sync(self, query: str) -> str:
        # Schema search is fast and uses in-memory FAISS, but semantic_search_schema might be blocking or async?
        # Checked ai_schema_search.py: semantic_search_schema is sync (CPU bound).
        try:
            results = self.schema_searcher.semantic_search_schema(query)
            formatted = "\n".join([
                f"- Table: {r['table_name']}, Col: {r['column_name']} ({r['data_type']}) - Score: {r.get('relevance_score', 0):.2f}" 
                for r in results
            ])
            return f"Relevant schema items:\n{formatted}"
        except Exception as e:
            return f"Error searching schema: {e}"

    async def _create_table_async(self, query: str) -> str:
        if not query.strip().upper().startswith("CREATE"):
            return "Error: Command must start with CREATE"
        return await self._execute_sql_async(query)

    async def _alter_table_async(self, query: str) -> str:
        if not query.strip().upper().startswith("ALTER"):
            return "Error: Command must start with ALTER"
        return await self._execute_sql_async(query)

    async def natural_language_query(self, prompt: str) -> str:
        try:
            # We use ainvoke which handles async tools
            result = await self.agent_executor.ainvoke({"input": prompt})
            return result["output"]
        except Exception as e:
            logger.error(f"Agent execution error: {e}")
            return f"Error processing query: {str(e)}"
