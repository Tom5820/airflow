"""
Ingestion State Manager for Bitbucket API (Simplified)
Handles state tracking in PostgreSQL with partition_date, entity_type, repo_slug, is_completed
"""
from typing import List, Dict
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

logger = logging.getLogger(__name__)


class IngestionStateManager:
    """Manages ingestion state in PostgreSQL for resumable API fetching"""
    
    def __init__(self, postgres_conn_id: str = "postgres_default", schema: str = "staging"):
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.table = f"{schema}.bitbucket_ingestion_state"
    
    def _get_hook(self) -> PostgresHook:
        return PostgresHook(postgres_conn_id=self.postgres_conn_id)
    
    def ensure_table_exists(self):
        """Create the tracking table if it doesn't exist"""
        hook = self._get_hook()
        create_sql = f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema};
        
        CREATE TABLE IF NOT EXISTS {self.table} (
            id SERIAL PRIMARY KEY,
            partition_date DATE NOT NULL,
            entity_type VARCHAR(50) NOT NULL,
            repo_slug VARCHAR(255) NOT NULL,
            is_completed BOOLEAN DEFAULT FALSE,
            UNIQUE(partition_date, entity_type, repo_slug)
        );
        
        CREATE INDEX IF NOT EXISTS idx_ingestion_state_lookup 
        ON {self.table}(partition_date, entity_type, is_completed);
        """
        hook.run(create_sql)
        logger.info(f"Ensured table {self.table} exists")
    
    def init_repos_for_partition(
        self, 
        partition_date: str,
        entity_type: str,
        repo_slugs: List[str]
    ) -> int:
        """
        Initialize repos for a partition date.
        Inserts new repos, skips already existing ones (ON CONFLICT DO NOTHING).
        
        Args:
            partition_date: Date string in format YYYY-MM-DD or YYYYMMDD
            entity_type: Type of entity (commits, pullrequests)
            repo_slugs: List of repository slugs
            
        Returns: Number of repos initialized
        """
        if not repo_slugs:
            return 0
        
        # Normalize date format
        if len(partition_date) == 8:  # YYYYMMDD
            partition_date = f"{partition_date[:4]}-{partition_date[4:6]}-{partition_date[6:8]}"
            
        hook = self._get_hook()
        
        # Build insert values
        values = []
        for repo_slug in repo_slugs:
            values.append(f"('{partition_date}', '{entity_type}', '{repo_slug}')")
        
        insert_sql = f"""
        INSERT INTO {self.table} (partition_date, entity_type, repo_slug)
        VALUES {', '.join(values)}
        ON CONFLICT (partition_date, entity_type, repo_slug) DO NOTHING
        """
        hook.run(insert_sql)
        logger.info(f"Initialized {len(repo_slugs)} repos for partition={partition_date}, entity_type={entity_type}")
        return len(repo_slugs)
    
    def get_pending_repos(self, partition_date: str, entity_type: str) -> List[str]:
        """Get list of repo_slugs that haven't been completed yet"""
        # Normalize date format
        if len(partition_date) == 8:  # YYYYMMDD
            partition_date = f"{partition_date[:4]}-{partition_date[4:6]}-{partition_date[6:8]}"
            
        hook = self._get_hook()
        
        sql = f"""
        SELECT repo_slug
        FROM {self.table}
        WHERE partition_date = %s AND entity_type = %s AND is_completed = FALSE
        ORDER BY id
        """
        result = hook.get_records(sql, parameters=(partition_date, entity_type))
        
        repos = [row[0] for row in result]
        logger.info(f"Found {len(repos)} pending repos for partition={partition_date}, entity_type={entity_type}")
        return repos
    
    def mark_repo_completed(self, partition_date: str, entity_type: str, repo_slug: str):
        """Mark a repo as successfully completed"""
        # Normalize date format
        if len(partition_date) == 8:  # YYYYMMDD
            partition_date = f"{partition_date[:4]}-{partition_date[4:6]}-{partition_date[6:8]}"
            
        hook = self._get_hook()
        
        sql = f"""
        UPDATE {self.table}
        SET is_completed = TRUE
        WHERE partition_date = %s AND entity_type = %s AND repo_slug = %s
        """
        hook.run(sql, parameters=(partition_date, entity_type, repo_slug))
        logger.info(f"Marked repo {repo_slug} as completed")
    
    def get_ingestion_summary(self, partition_date: str, entity_type: str) -> Dict:
        """Get summary statistics for an ingestion partition"""
        # Normalize date format
        if len(partition_date) == 8:  # YYYYMMDD
            partition_date = f"{partition_date[:4]}-{partition_date[4:6]}-{partition_date[6:8]}"
            
        hook = self._get_hook()
        
        sql = f"""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN is_completed THEN 1 ELSE 0 END) as completed
        FROM {self.table}
        WHERE partition_date = %s AND entity_type = %s
        """
        result = hook.get_first(sql, parameters=(partition_date, entity_type))
        
        total = result[0] or 0
        completed = result[1] or 0
        
        return {
            "total": total,
            "completed": completed,
            "pending": total - completed
        }
    
    def cleanup_old_partitions(self, days_to_keep: int = 7):
        """Clean up old ingestion state records"""
        hook = self._get_hook()
        
        sql = f"""
        DELETE FROM {self.table}
        WHERE partition_date < CURRENT_DATE - INTERVAL '{days_to_keep} days'
        """
        hook.run(sql)
        logger.info(f"Cleaned up ingestion states older than {days_to_keep} days")
