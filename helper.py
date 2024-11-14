import argparse
def get_s3_path(mode):
    if mode == "single_mb":
        return 's3://md-test-bucket-user-1/20_med_parquet_files/yellow_trip_data_med_1.parquet'
    elif mode == "multi_mb":
        return 's3://md-test-bucket-user-1/20_med_parquet_files/*.parquet'
    elif mode == "single_gb":
        return 's3://md-test-bucket-user-1/yellow_trip_data_1gb.parquet'
    else:
        raise ValueError(f"Unknown mode: {mode}")

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, required=True, help="mode to run the query in: single_mb, multi_mb, single_gb, etc.")
    parser.add_argument("--key_id", type=str, required=False, help="AWS access key ID")
    parser.add_argument("--secret", type=str, required=False, help="AWS secret access key")
    parser.add_argument("--md_token", type=str, required=False, help="MotherDuck token")
    parser.add_argument("--num_runs", type=int, default=10, help="number of times to run the query")

    # Parse arguments
    return parser.parse_args()