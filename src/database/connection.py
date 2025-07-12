"""
Database connection management for Supabase PostgreSQL.
"""
import logging
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from contextlib import contextmanager
from typing import Generator, Any, Dict, List
from src.config.database import get_database_config

# Configure logger
logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Manages database connections to Supabase PostgreSQL."""
    
    def __init__(self):
        """Initialize with database configuration."""
        self.config = get_database_config()
        self._engine = None
    
    @property
    def engine(self) -> Engine:
        """Get SQLAlchemy engine (lazy initialization)."""
        if self._engine is None:
            self._engine = create_engine(
                self.config.connection_string,
                pool_pre_ping=True,
                pool_recycle=300,
                echo=False  # Set to True for SQL debugging
            )
        return self._engine
    
    @contextmanager
    def get_connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """Context manager for psycopg2 database connections."""
        connection = None
        try:
            connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                dbname=self.config.dbname,
                user=self.config.user,
                password=self.config.password
            )
            logger.info("Database connection established successfully")
            yield connection
            
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {e}")
            if connection:
                connection.rollback()
            raise
            
        finally:
            if connection:
                connection.close()
                logger.info("Database connection closed")
    
    @contextmanager
    def get_cursor(self) -> Generator[psycopg2.extensions.cursor, None, None]:
        """Context manager for database cursor with automatic commit/rollback."""
        with self.get_connection() as connection:
            cursor = None
            try:
                cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                yield cursor
                connection.commit()
                
            except psycopg2.Error as e:
                logger.error(f"Database operation error: {e}")
                connection.rollback()
                raise
                
            finally:
                if cursor:
                    cursor.close()
    
    def test_connection(self) -> bool:
        """Test database connection and return success status."""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT NOW() as current_time;")
                result = cursor.fetchone()
                logger.info(f"Connection test successful. Current time: {result['current_time']}")
                return True
                
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def execute_query(self, query: str, params=None) -> List[Dict[str, Any]]:
        """Execute a SELECT query and return results as list of dictionaries."""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                logger.info(f"Query executed successfully. {len(results)} rows returned")
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    def execute_command(self, command: str, params=None) -> int:
        """Execute INSERT/UPDATE/DELETE command and return affected rows count."""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(command, params)
                affected_rows = cursor.rowcount
                logger.info(f"Command executed successfully. {affected_rows} rows affected")
                return affected_rows
                
        except Exception as e:
            logger.error(f"Command execution failed: {e}")
            raise
    
    def execute_sqlalchemy_query(self, query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Execute query using SQLAlchemy engine."""
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(query), params or {})
                rows = result.fetchall()
                # Convert to list of dictionaries
                return [dict(row._mapping) for row in rows]
                
        except Exception as e:
            logger.error(f"SQLAlchemy query execution failed: {e}")
            raise


# Global database instance
db = DatabaseConnection()


def get_db_connection() -> DatabaseConnection:
    """Get global database connection instance."""
    return db
