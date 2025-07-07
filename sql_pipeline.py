"""Simple ZenML pipeline demonstrating SQL query execution with custom materializer."""

from typing import Annotated
from zenml import step, pipeline
from sql_executor import SQLQuery
from sql_materializer import SQLQueryMaterializer


@step(output_materializers=SQLQueryMaterializer)
def create_sql_query() -> SQLQuery:
    """Create a sample SQL query for demonstration."""
    
    query = """
    SELECT 
        u.id,
        u.name,
        u.email,
        COUNT(o.id) as order_count,
        SUM(o.total_amount) as total_spent
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    WHERE u.created_at >= '2023-01-01'
    GROUP BY u.id, u.name, u.email
    HAVING COUNT(o.id) > 0
    ORDER BY total_spent DESC
    LIMIT 100
    """
    
    sql_query = SQLQuery(
        query=query.strip(),
        name="user_order_analytics",
        description="Analytics query to get user order statistics for users created after 2023-01-01",
        parameters={
            "start_date": "2023-01-01",
            "min_orders": 1,
            "limit": 100
        }
    )
    
    return sql_query


@step(output_materializers=SQLQueryMaterializer)
def create_complex_sql_query() -> SQLQuery:
    """Create a more complex SQL query with CTEs and window functions."""
    
    query = """
    WITH user_metrics AS (
        SELECT 
            user_id,
            COUNT(*) as total_orders,
            SUM(total_amount) as total_spent,
            AVG(total_amount) as avg_order_value,
            MIN(created_at) as first_order_date,
            MAX(created_at) as last_order_date
        FROM orders
        WHERE created_at >= '2023-01-01'
        GROUP BY user_id
    ),
    user_rankings AS (
        SELECT 
            user_id,
            total_orders,
            total_spent,
            avg_order_value,
            ROW_NUMBER() OVER (ORDER BY total_spent DESC) as spending_rank,
            ROW_NUMBER() OVER (ORDER BY total_orders DESC) as order_count_rank,
            NTILE(4) OVER (ORDER BY total_spent DESC) as spending_quartile
        FROM user_metrics
    )
    SELECT 
        u.name,
        u.email,
        ur.total_orders,
        ur.total_spent,
        ur.avg_order_value,
        ur.spending_rank,
        ur.order_count_rank,
        ur.spending_quartile,
        CASE 
            WHEN ur.spending_quartile = 1 THEN 'VIP'
            WHEN ur.spending_quartile = 2 THEN 'High Value'
            WHEN ur.spending_quartile = 3 THEN 'Medium Value'
            ELSE 'Low Value'
        END as customer_segment
    FROM users u
    JOIN user_rankings ur ON u.id = ur.user_id
    WHERE ur.spending_rank <= 50
    ORDER BY ur.spending_rank
    """
    
    sql_query = SQLQuery(
        query=query.strip(),
        name="customer_segmentation_analysis",
        description="Advanced customer segmentation analysis using CTEs and window functions",
        parameters={
            "start_date": "2023-01-01",
            "top_customers": 50
        }
    )
    
    return sql_query


@step
def execute_sql_query(query: SQLQuery) -> dict:
    """Execute the SQL query and return results."""
    
    print(f"Executing query: {query.name}")
    print(f"Description: {query.description}")
    print(f"Query length: {len(query.query)} characters")
    
    # Execute the query (mocked for demonstration)
    result = query.execute(mock=True)
    
    print(f"Execution completed with status: {result['status']}")
    print(f"Rows affected: {result.get('rows_affected', 'N/A')}")
    print(f"Execution time: {result.get('execution_time_ms', 'N/A')} ms")
    
    return result


@step
def analyze_query_performance(query: SQLQuery, execution_result: dict) -> dict:
    """Analyze query performance and provide recommendations."""
    
    analysis = {
        "query_name": query.name,
        "query_complexity": "medium",
        "performance_score": 85,
        "recommendations": [
            "Consider adding an index on users.created_at for better performance",
            "The LEFT JOIN might be optimized with proper indexing on orders.user_id",
            "LIMIT 100 helps control result set size"
        ],
        "estimated_cost": {
            "cpu_usage": "low",
            "memory_usage": "medium",
            "io_operations": "medium"
        }
    }
    
    # Analyze query complexity based on keywords
    query_upper = query.query.upper()
    complexity_indicators = {
        "CTE": "WITH" in query_upper,
        "window_functions": "OVER" in query_upper,
        "subqueries": "(" in query.query and "SELECT" in query_upper,
        "joins": any(join in query_upper for join in ["JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN"]),
        "aggregations": any(agg in query_upper for agg in ["COUNT", "SUM", "AVG", "MIN", "MAX"]),
        "having_clause": "HAVING" in query_upper
    }
    
    complexity_score = sum(complexity_indicators.values())
    
    if complexity_score >= 4:
        analysis["query_complexity"] = "high"
        analysis["performance_score"] = 70
    elif complexity_score >= 2:
        analysis["query_complexity"] = "medium"
        analysis["performance_score"] = 85
    else:
        analysis["query_complexity"] = "low"
        analysis["performance_score"] = 95
    
    analysis["complexity_indicators"] = complexity_indicators
    
    print(f"Query complexity analysis for '{query.name}':")
    print(f"  - Complexity: {analysis['query_complexity']}")
    print(f"  - Performance Score: {analysis['performance_score']}/100")
    print(f"  - Complexity Indicators: {complexity_indicators}")
    
    return analysis


@pipeline
def sql_execution_pipeline():
    """Pipeline that demonstrates SQL query execution with custom materializer."""
    
    # Create different types of SQL queries
    simple_query = create_sql_query()
    complex_query = create_complex_sql_query()
    
    # Execute the queries
    simple_result = execute_sql_query(simple_query)
    complex_result = execute_sql_query(complex_query)
    
    # Analyze query performance
    simple_analysis = analyze_query_performance(simple_query, simple_result)
    complex_analysis = analyze_query_performance(complex_query, complex_result)
    
    return simple_analysis, complex_analysis


if __name__ == "__main__":
    # Run the pipeline
    pipeline_run = sql_execution_pipeline()
    print("Pipeline execution completed successfully!")
    print("Check the ZenML dashboard to view SQL query visualizations and metadata.")