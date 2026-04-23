from etl_pipeline import ETLPipeline
from clickhouse_reports import ClickHouseReports
import time


def main():
    print("\n=== Loading data into Star Schema ===")
    etl = ETLPipeline()
    df = etl.read_mock_data()
    etl.transform_and_load_star_schema(df)
    print("Star schema loading completed!")

    time.sleep(5)

    print("\n=== Creating reports in ClickHouse ===")
    ch_reports = ClickHouseReports()
    ch_reports.create_reports()
    print("ClickHouse reports creation completed!")

    print("\n=== All tasks completed successfully! ===")


if __name__ == "__main__":
    main()
