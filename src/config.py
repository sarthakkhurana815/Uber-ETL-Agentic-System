
foundry_api_key = ""
foundry_endpoint = ""
foundry_model = "gpt-4.1-mini"

storage_base = "file:/Workspace/UBER/uber-taxi-agentic-etl/data"

source_path = f"{storage_base}/ncr_ride_bookings.csv"
base_path = "/Volumes/dbws/default/etl_output"
bronze_path = f"{base_path}/bronze"
silver_path = f"{base_path}/silver"

# Uber ETL ke liye quarantine zaroori hai
quarantine_path = f"{base_path}/quarantine"

gold_daily_path = f"{base_path}/gold/daily_trip_summary"
gold_location_path = f"{base_path}/gold/location_trip_summary"
gold_payment_path = f"{base_path}/gold/payment_type_trip_summary"

# "/Workspace/green-taxi-agentic-etl/data"

