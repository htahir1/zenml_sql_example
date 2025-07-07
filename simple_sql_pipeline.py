"""Simple ZenML pipeline for executing SQL scripts in sequence."""

from zenml import step, pipeline
from zenml.client import Client
from typing import Dict, Any
import time


@step
def execute_sql_script(script_name: str, query: str) -> Dict[str, Any]:
    """Execute a SQL script and return results."""
    
    print(f"Executing SQL script: {script_name}")
    print(f"Query: {query[:100]}...")
    
    try:
        # Get database credentials from ZenML secrets
        client = Client()
        
        # Try to get database credentials (will use mock if not available)
        try:
            db_secret = client.get_secret("db_credentials")
            print(f"Using database credentials for host: {db_secret.secret_values.get('host', 'unknown')}")
        except Exception:
            print("No database credentials found, using mock execution")
            db_secret = None
        
        # Mock SQL execution for demonstration
        start_time = time.time()
        
        # Simulate database work
        time.sleep(0.1)
        
        execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds
        
        # Mock result based on query type
        if "SELECT" in query.upper():
            rows_affected = 0
            result_type = "query"
            sample_data = [
                {"id": 1, "name": "John Doe", "status": "active"},
                {"id": 2, "name": "Jane Smith", "status": "active"},
                {"id": 3, "name": "Bob Johnson", "status": "inactive"}
            ]
        elif "INSERT" in query.upper():
            rows_affected = 5
            result_type = "insert"
            sample_data = []
        elif "UPDATE" in query.upper():
            rows_affected = 12
            result_type = "update"
            sample_data = []
        elif "DELETE" in query.upper():
            rows_affected = 3
            result_type = "delete"
            sample_data = []
        else:
            rows_affected = 0
            result_type = "unknown"
            sample_data = []
        
        result = {
            "script_name": script_name,
            "status": "success",
            "rows_affected": rows_affected,
            "execution_time_ms": round(execution_time, 2),
            "result_type": result_type,
            "sample_data": sample_data,
            "query_length": len(query)
        }
        
        print(f"✅ Script '{script_name}' executed successfully")
        print(f"   - Rows affected: {rows_affected}")
        print(f"   - Execution time: {execution_time:.2f}ms")
        
        return result
        
    except Exception as e:
        print(f"❌ Error executing script '{script_name}': {str(e)}")
        return {
            "script_name": script_name,
            "status": "error",
            "error_message": str(e),
            "rows_affected": 0,
            "execution_time_ms": 0
        }


@step
def validate_results(results: Dict[str, Any]) -> bool:
    """Validate SQL execution results."""
    
    if results["status"] != "success":
        print(f"❌ Validation failed for {results['script_name']}: {results.get('error_message', 'Unknown error')}")
        return False
    
    print(f"✅ Validation passed for {results['script_name']}")
    return True


@pipeline
def simple_sql_pipeline():
    """Pipeline that executes multiple SQL scripts in sequence."""
    
    # Define your SQL scripts
    scripts = [
        {
            "name": "create_users_table",
            "query": """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(50) DEFAULT 'active'
            )
            """
        },
        {
            "name": "insert_sample_users",
            "query": """
            INSERT INTO users (name, email) VALUES
            ('John Doe', 'john@example.com'),
            ('Jane Smith', 'jane@example.com'),
            ('Bob Johnson', 'bob@example.com'),
            ('Alice Brown', 'alice@example.com'),
            ('Charlie Wilson', 'charlie@example.com')
            """
        },
        {
            "name": "update_user_status",
            "query": """
            UPDATE users 
            SET status = 'premium' 
            WHERE email IN ('john@example.com', 'jane@example.com')
            """
        },
        {
            "name": "query_active_users",
            "query": """
            SELECT id, name, email, status, created_at
            FROM users 
            WHERE status = 'active' 
            ORDER BY created_at DESC
            """
        },
        {
            "name": "cleanup_inactive_users",
            "query": """
            DELETE FROM users 
            WHERE status = 'inactive' 
            AND created_at < NOW() - INTERVAL '30 days'
            """
        }
    ]
    
    # Execute scripts in sequence
    results = []
    
    for script in scripts:
        result = execute_sql_script(script["name"], script["query"])
        is_valid = validate_results(result)
        
        # Store result for final summary
        results.append(result)
        
        # Stop pipeline if validation fails
        if not is_valid:
            print(f"🛑 Pipeline stopped due to validation failure in {script['name']}")
            break
    
    return results


if __name__ == "__main__":
    # Run the pipeline
    print("🚀 Starting Simple SQL Pipeline...")
    print("=" * 50)
    
    pipeline_results = simple_sql_pipeline()
    
    print("\n" + "=" * 50)
    print("📊 Pipeline Execution Summary:")
    print("=" * 50)
    
    total_execution_time = 0
    successful_scripts = 0
    
    for result in pipeline_results:
        status_icon = "✅" if result["status"] == "success" else "❌"
        print(f"{status_icon} {result['script_name']}")
        print(f"   Status: {result['status']}")
        print(f"   Rows affected: {result.get('rows_affected', 0)}")
        print(f"   Execution time: {result.get('execution_time_ms', 0)}ms")
        print()
        
        if result["status"] == "success":
            successful_scripts += 1
            total_execution_time += result.get("execution_time_ms", 0)
    
    print(f"📈 Total successful scripts: {successful_scripts}/{len(pipeline_results)}")
    print(f"⏱️  Total execution time: {total_execution_time:.2f}ms")
    print("\n✨ Pipeline execution completed!")