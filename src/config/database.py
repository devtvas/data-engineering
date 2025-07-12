"""
Database configuration for Supabase PostgreSQL connection.
"""
import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


@dataclass
class DatabaseConfig:
    """Configuration class for database connection parameters."""
    host: str
    port: int
    dbname: str
    user: str
    password: str
    
    @classmethod
    def from_env(cls) -> 'DatabaseConfig':
        """Create DatabaseConfig from environment variables."""
        return cls(
            host=os.getenv("SUPABASE_HOST", ""),
            port=int(os.getenv("SUPABASE_PORT", "5432")),
            dbname=os.getenv("SUPABASE_DBNAME", "postgres"),
            user=os.getenv("SUPABASE_USER", "postgres"),
            password=os.getenv("SUPABASE_PASSWORD", "")
        )
    
    @property
    def connection_string(self) -> str:
        """Generate PostgreSQL connection string."""
        return (f"postgresql://{self.user}:{self.password}@"
                f"{self.host}:{self.port}/{self.dbname}")
    
    def validate(self) -> bool:
        """Validate that all required fields are present."""
        required_fields = [self.host, self.password]
        return all(field.strip() for field in required_fields)


def get_database_config() -> DatabaseConfig:
    """Get validated database configuration."""
    config = DatabaseConfig.from_env()
    
    if not config.validate():
        raise ValueError(
            "Missing required database configuration. "
            "Please check your .env file and ensure SUPABASE_HOST "
            "and SUPABASE_PASSWORD are set."
        )
    
    return config
