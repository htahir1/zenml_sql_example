"""SQL Executor class for ZenML pipelines."""

import json
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime
from zenml.client import Client


@dataclass
class SQLQuery:
    """A SQL query with metadata and execution context."""
    
    query: str
    name: Optional[str] = None
    description: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.name is None:
            self.name = f"query_{self.created_at.strftime('%Y%m%d_%H%M%S')}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "query": self.query,
            "name": self.name,
            "description": self.description,
            "parameters": self.parameters,
            "created_at": self.created_at.isoformat() if self.created_at else None
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SQLQuery":
        """Create from dictionary."""
        created_at = None
        if data.get("created_at"):
            created_at = datetime.fromisoformat(data["created_at"])
        
        return cls(
            query=data["query"],
            name=data.get("name"),
            description=data.get("description"),
            parameters=data.get("parameters"),
            created_at=created_at
        )
    
    def execute(self, mock: bool = True) -> Dict[str, Any]:
        """Execute the SQL query."""
        if mock:
            return self._mock_execute()
        else:
            return self._real_execute()
    
    def _mock_execute(self) -> Dict[str, Any]:
        """Mock SQL execution for demonstration."""
        return {
            "status": "success",
            "rows_affected": 42,
            "execution_time_ms": 150,
            "result_preview": [
                {"id": 1, "name": "John Doe", "email": "john@example.com"},
                {"id": 2, "name": "Jane Smith", "email": "jane@example.com"},
                {"id": 3, "name": "Bob Johnson", "email": "bob@example.com"}
            ],
            "metadata": {
                "query_hash": hash(self.query),
                "executed_at": datetime.utcnow().isoformat()
            }
        }
    
    def _real_execute(self) -> Dict[str, Any]:
        """Real SQL execution using BigQuery secrets."""
        try:
            # Fetch BigQuery credentials from ZenML secrets
            client = Client()
            bq_secret = client.get_secret("bigquery_credentials")
            
            # Mock BigQuery connection setup
            credentials = {
                "project_id": bq_secret.secret_values.get("project_id"),
                "private_key": bq_secret.secret_values.get("private_key"),
                "client_email": bq_secret.secret_values.get("client_email")
            }
            
            return {
                "status": "success",
                "message": f"Would execute query using BigQuery project: {credentials['project_id']}",
                "query": self.query,
                "mock": True
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to execute query: {str(e)}",
                "query": self.query
            }
    
    def to_html(self) -> str:
        """Generate HTML visualization of the query."""
        execution_result = self.execute(mock=True)
        
        html = f"""
        <div style="font-family: Arial, sans-serif; max-width: 800px; margin: 0 auto;">
            <h2 style="color: #333;">SQL Query: {self.name}</h2>
            
            <div style="background: #f5f5f5; padding: 15px; border-radius: 5px; margin: 10px 0;">
                <h3 style="color: #666; margin-top: 0;">Query</h3>
                <pre style="background: #fff; padding: 10px; border-left: 4px solid #007acc; overflow-x: auto;"><code>{self.query}</code></pre>
            </div>
            
            {f'<p><strong>Description:</strong> {self.description}</p>' if self.description else ''}
            
            <div style="background: #e8f5e8; padding: 15px; border-radius: 5px; margin: 10px 0;">
                <h3 style="color: #2d5a2d; margin-top: 0;">Execution Results</h3>
                <p><strong>Status:</strong> {execution_result['status']}</p>
                <p><strong>Rows Affected:</strong> {execution_result.get('rows_affected', 'N/A')}</p>
                <p><strong>Execution Time:</strong> {execution_result.get('execution_time_ms', 'N/A')} ms</p>
                
                {self._format_result_table(execution_result.get('result_preview', []))}
            </div>
            
            <div style="background: #f0f8ff; padding: 15px; border-radius: 5px; margin: 10px 0;">
                <h3 style="color: #1e3a8a; margin-top: 0;">Metadata</h3>
                <p><strong>Created At:</strong> {self.created_at}</p>
                <p><strong>Query Hash:</strong> {hash(self.query)}</p>
                {f'<p><strong>Parameters:</strong> {json.dumps(self.parameters, indent=2)}</p>' if self.parameters else ''}
            </div>
        </div>
        """
        return html
    
    def _format_result_table(self, results: List[Dict[str, Any]]) -> str:
        """Format query results as HTML table."""
        if not results:
            return "<p>No results to display.</p>"
        
        headers = list(results[0].keys())
        
        table_html = '<table style="width: 100%; border-collapse: collapse; margin: 10px 0;">'
        table_html += '<thead><tr>'
        for header in headers:
            table_html += f'<th style="border: 1px solid #ddd; padding: 8px; background: #f2f2f2;">{header}</th>'
        table_html += '</tr></thead><tbody>'
        
        for row in results:
            table_html += '<tr>'
            for header in headers:
                table_html += f'<td style="border: 1px solid #ddd; padding: 8px;">{row.get(header, "")}</td>'
            table_html += '</tr>'
        
        table_html += '</tbody></table>'
        return table_html