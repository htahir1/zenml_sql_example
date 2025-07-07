"""Custom materializer for SQL queries in ZenML pipelines."""

import os
import json
from typing import Type, Any, Dict
from zenml.materializers.base_materializer import BaseMaterializer
from zenml.enums import ArtifactType, VisualizationType
from zenml.metadata.metadata_types import MetadataType
from sql_executor import SQLQuery


class SQLQueryMaterializer(BaseMaterializer):
    """Materializer for SQLQuery objects."""
    
    ASSOCIATED_TYPES = (SQLQuery,)
    ASSOCIATED_ARTIFACT_TYPE = ArtifactType.DATA
    
    def load(self, data_type: Type[Any]) -> SQLQuery:
        """Load SQLQuery from storage."""
        filepath = os.path.join(self.uri, "sql_query.json")
        
        with self.artifact_store.open(filepath, "r") as f:
            data = json.load(f)
        
        return SQLQuery.from_dict(data)
    
    def save(self, data: SQLQuery) -> None:
        """Save SQLQuery to storage."""
        filepath = os.path.join(self.uri, "sql_query.json")
        
        with self.artifact_store.open(filepath, "w") as f:
            json.dump(data.to_dict(), f, indent=2)
    
    def save_visualizations(self, data: SQLQuery) -> Dict[str, VisualizationType]:
        """Generate visualizations for the ZenML dashboard."""
        visualizations = {}
        
        # HTML visualization
        html_path = os.path.join(self.uri, "sql_query_visualization.html")
        with self.artifact_store.open(html_path, "w") as f:
            f.write(data.to_html())
        visualizations[html_path] = VisualizationType.HTML
        
        # Markdown summary
        markdown_path = os.path.join(self.uri, "sql_query_summary.md")
        with self.artifact_store.open(markdown_path, "w") as f:
            f.write(self._generate_markdown_summary(data))
        visualizations[markdown_path] = VisualizationType.MARKDOWN
        
        # CSV with query metadata
        csv_path = os.path.join(self.uri, "query_metadata.csv")
        with self.artifact_store.open(csv_path, "w") as f:
            f.write(self._generate_csv_metadata(data))
        visualizations[csv_path] = VisualizationType.CSV
        
        return visualizations
    
    def extract_metadata(self, data: SQLQuery) -> Dict[str, MetadataType]:
        """Extract metadata for tracking and search."""
        execution_result = data.execute(mock=True)
        
        metadata = {
            "query_name": data.name,
            "query_length": len(data.query),
            "query_hash": str(hash(data.query)),
            "has_parameters": bool(data.parameters),
            "parameter_count": len(data.parameters) if data.parameters else 0,
            "execution_status": execution_result.get("status", "unknown"),
            "rows_affected": execution_result.get("rows_affected", 0),
            "execution_time_ms": execution_result.get("execution_time_ms", 0),
            "created_at": data.created_at.isoformat() if data.created_at else None,
        }
        
        if data.description:
            metadata["description"] = data.description
        
        # Extract SQL keywords for searchability
        sql_keywords = self._extract_sql_keywords(data.query)
        if sql_keywords:
            metadata["sql_keywords"] = ", ".join(sql_keywords)
        
        return metadata
    
    def _generate_markdown_summary(self, data: SQLQuery) -> str:
        """Generate a markdown summary of the SQL query."""
        execution_result = data.execute(mock=True)
        
        markdown = f"""# SQL Query: {data.name}

## Query Details
- **Name:** {data.name}
- **Created:** {data.created_at}
- **Status:** {execution_result.get('status', 'unknown')}

## SQL Query
```sql
{data.query}
```

## Description
{data.description or 'No description provided'}

## Execution Results
- **Rows Affected:** {execution_result.get('rows_affected', 'N/A')}
- **Execution Time:** {execution_result.get('execution_time_ms', 'N/A')} ms
- **Status:** {execution_result.get('status', 'unknown')}

## Parameters
{json.dumps(data.parameters, indent=2) if data.parameters else 'No parameters'}

## Metadata
- **Query Hash:** {hash(data.query)}
- **SQL Keywords:** {', '.join(self._extract_sql_keywords(data.query))}
"""
        return markdown
    
    def _generate_csv_metadata(self, data: SQLQuery) -> str:
        """Generate CSV with query metadata."""
        execution_result = data.execute(mock=True)
        
        csv_content = "attribute,value\\n"
        csv_content += f"name,{data.name}\\n"
        csv_content += f"query_length,{len(data.query)}\\n"
        csv_content += f"query_hash,{hash(data.query)}\\n"
        csv_content += f"created_at,{data.created_at}\\n"
        csv_content += f"execution_status,{execution_result.get('status', 'unknown')}\\n"
        csv_content += f"rows_affected,{execution_result.get('rows_affected', 0)}\\n"
        csv_content += f"execution_time_ms,{execution_result.get('execution_time_ms', 0)}\\n"
        csv_content += f"has_parameters,{bool(data.parameters)}\\n"
        csv_content += f"parameter_count,{len(data.parameters) if data.parameters else 0}\\n"
        
        return csv_content
    
    def _extract_sql_keywords(self, query: str) -> list:
        """Extract SQL keywords from the query for metadata."""
        sql_keywords = [
            'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN',
            'GROUP BY', 'ORDER BY', 'HAVING', 'INSERT', 'UPDATE', 'DELETE', 'CREATE',
            'ALTER', 'DROP', 'INDEX', 'TABLE', 'VIEW', 'PROCEDURE', 'FUNCTION',
            'UNION', 'INTERSECT', 'EXCEPT', 'WITH', 'CTE', 'WINDOW', 'OVER',
            'PARTITION BY', 'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'COUNT', 'SUM',
            'AVG', 'MIN', 'MAX', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END'
        ]
        
        query_upper = query.upper()
        found_keywords = []
        
        for keyword in sql_keywords:
            if keyword in query_upper:
                found_keywords.append(keyword)
        
        return found_keywords