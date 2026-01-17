# Supabase AI MCP Server

![Python Version](https://img.shields.io/badge/python-3.11-blue.svg)
![Exposed Port](https://img.shields.io/badge/port-8000-green.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Docker](https://img.shields.io/badge/docker-ready-blue.svg)

A production-ready [Model Context Protocol (MCP)](https://modelcontextprotocol.io) server for Supabase, enhanced with advanced AI capabilities for semantic search, natural language querying, and automated migration generation.

---

## 🚀 Features

- **🧠 Natural Language Database Queries**
  Ask complex questions about your data in plain English. The AI agent executes SQL and interprets results.
  
- **🔍 Semantic Schema Search**
  Find tables and columns using meaning rather than exact keywords (e.g., search "user contact info" to find `users.phone` and `users.email`). Powered by **FAISS** and **Sentence Transformers**.

- **🪄 Automated Migration Generation**
  Describe schema changes in natural language, and get valid, safe SQL migration scripts automatically generated.

- **🔌 Seamless MCP Integration**
  Fully compatible with the Model Context Protocol for easy integration with Claude Desktop and other MCP clients.

- **🛡️ Safe Execution**
  Built-in SQL validation and safety modes (read-only by default) to prevent accidental data loss.

- **🐳 Docker Ready**
  One-command deployment with Docker and Docker Compose.

---

## 🛠️ Tech Stack

- **Server**: Model Context Protocol (MCP) Python SDK
- **AI/LLM**: Anthropic Claude 3.5 Sonnet (via LangChain)
- **Vector Search**: FAISS (Facebook AI Similarity Search) + Sentence Transformers
- **Database**: Supabase (PostgreSQL)
- **Runtime**: Docker / Python 3.11

---

## 🏗️ Architecture

```mermaid
graph TD
    Client[MCP Client\n(Claude/IDE)] <--> Server[MCP Server\n(src.server)]
    
    subgraph "AI Core"
        Server <--> Agent[AI Agent\n(LangChain)]
        Server <--> Search[Schema Search\n(FAISS)]
        Server <--> Gen[Migration Gen\n(Claude)]
    end
    
    subgraph "Data Layer"
        Agent <--> DB[(Supabase PG)]
        Search <--> DB
        Gen <--> DB
    end
```

*(Note: ASCII fallback below if mermaid renders incorrectly)*

```
                  +----------------+
                  |  MCP Client    |
                  | (Claude/IDE)   |
                  +-------+--------+
                          |
                  +-------+--------+
                  |   MCP Server   |
                  |  (src.server)  |
      +-----------+-------+--------+-----------+
      |                   |                    |
+-----+------+     +------+-------+     +------+-------+
|  AI Agent  |     | Schema Search|     | Migration Gen|
| (LangChain)|     |   (FAISS)    |     |   (Claude)   |
+-----+------+     +------+-------+     +------+-------+
      |                   |                    |
      +-----------+-------+--------------------+
                  |
          +-------v--------+
          | Supabase (PG)  |
          +----------------+
```

---

## 📦 Installation

### Prerequisites
- Python 3.11+
- Docker (optional)
- Supabase Project credentials
- Anthropic API Key

### Local Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/saicharanrajoju/supabase-mcp-ai.git
   cd supabase-mcp-ai
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment:**
   Copy `.env.example` to `.env` and fill in your credentials.
   ```bash
   cp .env.example .env
   ```

4. **Run the server:**
   ```bash
   python -m src.server
   ```

### Docker Deployment

1. **Set up `.env` as above.**

2. **Run with Docker Compose:**
   ```bash
   docker-compose up --build -d
   ```

---

## ⚙️ Configuration

File: `.env`

| Variable | Description |
|----------|-------------|
| `ANTHROPIC_API_KEY` | Your Anthropic API key (required for AI features) |
| `SUPABASE_URL` | Your Supabase Project URL |
| `SUPABASE_KEY` | Your Supabase Service Role Key (or Anon Key for limited access) |

---

## 💡 Usage

### 🤖 AI Tools

#### `ai_query`
Ask questions about your data.
- **Input**: "How many users signed up last week?"
- **Output**: "There were 145 signups last week. Here is the breakdown..."

#### `search_schema`
Find relevant database structure.
- **Input**: "Where is user billing address stored?"
- **Output**: Returns `user_addresses`, `billing_profiles` tables with relevance scores.

#### `generate_migration`
Generate SQL for schema changes.
- **Input**: "Add distinct status column to orders table"
- **Output**: 
  ```sql
  ALTER TABLE orders ADD COLUMN status VARCHAR(50);
  ```

### 🛠️ Standard Tools
The server also provides standard Supabase tools:
- `read_query`: Execute read-only SQL.
- `write_query`: Execute write SQL (requires safety check).
- `get_tables`: List tables in a schema.

---

## 📂 Development Structure

- `src/server.py`: Entry point and tool registration.
- `src/ai_schema_search.py`: Vector database logic.
- `src/langchain_agents.py`: ReAct agent implementation.
- `src/nl_migrations.py`: Migration generation logic.
- `src/services/`: Core Supabase services.


