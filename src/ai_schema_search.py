
import os
import json
import logging
from typing import List, Dict, Any, Optional
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer
from src.services.database.postgres_client import PostgresClient, QueryResult
from src.services.database.sql.validator import SQLValidator
from src.logger import logger

class SchemaSearcher:
    """
    FAISS-based semantic schema search system
    """
    def __init__(self, postgres_client: PostgresClient):
        self.postgres_client = postgres_client
        self.model_name = "sentence-transformers/all-MiniLM-L6-v2"
        self.model = None # Lazy load
        self.index = None
        self.metadata: List[Dict] = []
        self.validator = SQLValidator()
        self.cache_dir = ".faiss_index"
        self.index_path = os.path.join(self.cache_dir, "schema.index")
        self.metadata_path = os.path.join(self.cache_dir, "metadata.json")

    async def initialize(self):
        """Load schema and build/load index"""
        logger.info("Initializing SchemaSearcher...")
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
        
        # Load model
        try:
            self.model = SentenceTransformer(self.model_name)
            logger.debug(f"Loaded model: {self.model_name}")
        except Exception as e:
            logger.error(f"Failed to load sentence-transformer model: {e}")
            raise

        # Check for cached index
        if os.path.exists(self.index_path) and os.path.exists(self.metadata_path):
            try:
                self.load_index_from_disk()
                logger.info("Loaded schema index from disk")
                return
            except Exception as e:
                logger.warning(f"Failed to load cached index, rebuilding: {e}")

        await self.refresh_index()

    async def refresh_index(self):
        """Fetch schema from DB, embed, and build index"""
        logger.info("Refreshing schema index...")
        schema_data = await self.fetch_schema()
        
        if not schema_data:
            logger.warning("No schema data found to index")
            return

        texts = []
        self.metadata = []

        for item in schema_data:
            # Create a rich textual representation for embedding
            text = f"Table: {item['table_name']} | Column: {item['column_name']} | Type: {item['data_type']}"
            if item.get('column_description'):
                text += f" | Column Description: {item['column_description']}"
            if item.get('table_description'):
                text += f" | Table Description: {item['table_description']}"
            
            texts.append(text)
            self.metadata.append(item)

        if texts:
            embeddings = self.model.encode(texts)
            dimension = embeddings.shape[1]
            
            self.index = faiss.IndexFlatL2(dimension)
            self.index.add(np.array(embeddings).astype('float32'))
            
            self.save_index_to_disk()
            logger.info(f"Indexed {len(texts)} schema items")

    async def fetch_schema(self) -> List[Dict]:
        """Query Supabase for schema information"""
        query = """
        SELECT 
            t.table_schema,
            t.table_name,
            c.column_name,
            c.data_type,
            pg_catalog.col_description(format('%s.%s', t.table_schema, t.table_name)::regclass::oid, c.ordinal_position) as column_description,
            pg_catalog.obj_description(format('%s.%s', t.table_schema, t.table_name)::regclass::oid, 'pg_class') as table_description
        FROM information_schema.tables t
        JOIN information_schema.columns c ON t.table_name = c.table_name AND t.table_schema = c.table_schema
        WHERE t.table_schema NOT IN ('information_schema', 'pg_catalog', 'auth', 'storage', 'graphql_public', 'pg_toast')
        AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_schema, t.table_name, c.ordinal_position;
        """
        
        try:
            # We use manually validated query to bypass normal restrictions if necessary, 
            # though this is a safe SELECT.
            validated = self.validator.validate_query(query)
            result = await self.postgres_client.execute_query(validated, readonly=True)
            
            # Helper to extract rows from QueryResult
            rows = []
            for stmt_result in result.results:
                rows.extend(stmt_result.rows)
            return rows
        except Exception as e:
            logger.error(f"Error fetching schema: {e}")
            return []

    def save_index_to_disk(self):
        try:
            if self.index:
                faiss.write_index(self.index, self.index_path)
            with open(self.metadata_path, 'w') as f:
                json.dump(self.metadata, f)
        except Exception as e:
            logger.error(f"Error saving index to disk: {e}")

    def load_index_from_disk(self):
        self.index = faiss.read_index(self.index_path)
        with open(self.metadata_path, 'r') as f:
            self.metadata = json.load(f)

    def semantic_search_schema(self, query: str, top_k: int = 5) -> List[Dict]:
        """
        Search schema using semantic similarity
        """
        if not self.index or not self.model:
            logger.warning("Search called before index initialization")
            return []

        try:
            query_vector = self.model.encode([query])
            distances, indices = self.index.search(np.array(query_vector).astype('float32'), top_k)
            
            results = []
            for i, idx in enumerate(indices[0]):
                if idx < len(self.metadata) and idx >= 0:
                    item = self.metadata[idx].copy()
                    item['relevance_score'] = float(1.0 / (1.0 + distances[0][i])) # Convert distance to score-like metric
                    results.append(item)
            
            return results
        except Exception as e:
            logger.error(f"Error during semantic search: {e}")
            return []
