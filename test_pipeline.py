import pandas as pd
from datetime import datetime, timedelta
from pipelines import run_pipeline

# Create sample source data
source_data = {
    'customer_id': [1, 2, 3, 4],
    'customer_name': ['john doe', 'jane smith', 'bob wilson', 'alice johnson'],
    'email': ['john@example.com', 'jane@example.com', 'bob@example.com', 'alice@example.com'],
    'status': ['ACTIVE', 'ACTIVE', 'INACTIVE', 'ACTIVE'],
    'salary': [50000, 60000, 55000, 70000]
}
source_df = pd.DataFrame(source_data)

# Create sample target data (SCD2 history table)
target_data = {
    'customer_id': [1, 2, 3],
    'customer_name': ['JOHN DOE', 'JANE SMITH', 'BOB WILSON'],
    'email': ['john@example.com', 'jane@example.com', 'bob@example.com'],
    'status': ['ACTIVE', 'ACTIVE', 'INACTIVE'],
    'salary': [50000, 60000, 55000],
    'is_current': [True, True, True],
    'start_date': [datetime.now() - timedelta(days=30)] * 3,
    'end_date': [datetime(9999, 12, 31)] * 3,
    'is_deleted': [False, False, False]
}
target_df = pd.DataFrame(target_data)

# Sample configuration
config = {
    "schema": {
        "target_columns": ['customer_id', 'customer_name', 'email', 'status', 'salary']
    },
    "transformations": {
        "uppercase": ["customer_name"],
        "filter": "status == 'ACTIVE'"
    },
    "dq": {
        "primary_key": "customer_id",
        "checks": []
    },
    "scd2": {
        "pk": "customer_id",
        "compare_columns": ["email", "salary"]
    }
}

print("=" * 60)
print("SOURCE DATA:")
print("=" * 60)
print(source_df)

print("\n" + "=" * 60)
print("TARGET DATA (BEFORE PIPELINE):")
print("=" * 60)
print(target_df)

# Run pipeline
result = run_pipeline(source_df, target_df, config)

print("\n" + "=" * 60)
print("RESULT (AFTER PIPELINE):")
print("=" * 60)
print(result)
print("\nShape:", result.shape)
print("Columns:", result.columns.tolist())
