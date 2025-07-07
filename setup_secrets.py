"""Script to set up ZenML secrets for BigQuery connection."""

import os
from zenml.client import Client


def setup_bigquery_secret():
    """Set up BigQuery credentials as a ZenML secret."""
    
    # Mock BigQuery credentials for demonstration
    mock_credentials = {
        "project_id": "my-bigquery-project",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMOCK_PRIVATE_KEY_HERE\n-----END PRIVATE KEY-----",
        "client_email": "service-account@my-bigquery-project.iam.gserviceaccount.com",
        "client_id": "123456789012345678901",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/service-account%40my-bigquery-project.iam.gserviceaccount.com"
    }
    
    try:
        client = Client()
        
        # Check if secret already exists
        try:
            existing_secret = client.get_secret("bigquery_credentials")
            print("BigQuery credentials secret already exists!")
            print(f"Secret contains keys: {list(existing_secret.secret_values.keys())}")
            return
        except:
            pass
        
        # Create the secret
        from zenml.models import SecretRequest
        
        secret_request = SecretRequest(
            name="bigquery_credentials",
            values=mock_credentials,
            scope="user"
        )
        
        client.create_secret(secret_request)
        print("✅ BigQuery credentials secret created successfully!")
        print("Secret name: bigquery_credentials")
        print(f"Secret contains keys: {list(mock_credentials.keys())}")
        
    except Exception as e:
        print(f"❌ Failed to create BigQuery secret: {e}")
        print("You can create it manually using the ZenML CLI:")
        print("zenml secret create bigquery_credentials --project_id=my-bigquery-project --client_email=service-account@my-bigquery-project.iam.gserviceaccount.com")


if __name__ == "__main__":
    setup_bigquery_secret()