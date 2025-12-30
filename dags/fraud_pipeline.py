with DAG(
    dag_id="fraud_detection_weekly_pipeline",
    schedule_interval="@weekly",
    start_date=...,
    catchup=False,
) as dag:

    detect_new_data = PythonOperator(
        task_id="detect_new_week_data",
        python_callable=check_s3_for_new_partition,
    )

    pull_data = PythonOperator(
        task_id="pull_data_from_s3",
        python_callable=download_weekly_data,
    )

    validate_schema = PythonOperator(
        task_id="validate_data_contract",
        python_callable=validate_against_schema,
    )

    detect_new_data >> pull_data >> validate_schema
