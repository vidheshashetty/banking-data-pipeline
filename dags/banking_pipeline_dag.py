from airflow import DAG 
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator 
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'vidhesha',
    'start_date': datetime(2026, 2, 2),
    'retries': 1,
    'end_date': datetime(2026, 6, 6),
    'retry_delay': timedelta(minutes=1),
    'email': ['vidheshashetty@gmail.com'],
    'email_on_retry':False,
    'email_on_failure':True
}

with DAG(
    dag_id='banking_pipeline',
    default_args=default_args,
    schedule='@daily',  
    catchup=False
) as dag:

    load_raw = SQLExecuteQueryOperator(
    task_id='load_raw',
    conn_id='snowflake_default',
    sql="""
    INSERT INTO RAW_Transactions (
        trans_id,
        trans_timestamp,
        acct_number,
        customer_id,
        trans_type,
        amount,
        currency,
        sender_bank,
        receiver_bank,
        status,
        load_date
    )
    SELECT
        UUID_STRING(),
        DATEADD(
            day,
            -UNIFORM(0,5,RANDOM()),
            CURRENT_TIMESTAMP()
        ),
        'ACCT' || UNIFORM(100000,999999,RANDOM()),
        'CUST' || UNIFORM(1000,9999,RANDOM()),
        CASE 
            WHEN UNIFORM(1,10,RANDOM()) > 5 THEN 'DEBIT'
            ELSE 'CREDIT'
        END,
        ROUND(UNIFORM(-500,10000,RANDOM()),2),
        'INR',
        CASE 
            WHEN UNIFORM(1,10,RANDOM()) > 5 THEN 'HDFC'
            ELSE 'ICICI'
        END,
        CASE 
            WHEN UNIFORM(1,10,RANDOM()) > 5 THEN 'SBI'
            ELSE 'AXIS'
        END,
        CASE 
            WHEN UNIFORM(1,10,RANDOM()) > 3 THEN 'SUCCESS'
            ELSE 'FAILED'
        END,
        CURRENT_DATE()
    FROM TABLE(GENERATOR(ROWCOUNT => 100));
    """
    )

    clean_data = SQLExecuteQueryOperator(
        task_id='clean_data',
        conn_id='snowflake_default',
        sql="""
        MERGE INTO CLEAN_Transactions t
        USING (
            SELECT *
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                    PARTITION BY trans_id
                    ORDER BY trans_timestamp DESC
                ) AS rn
            FROM RAW_Transactions
            WHERE processed_flag = FALSE
                AND trans_id IS NOT NULL
                AND amount > 0
                AND trans_type IN ('DEBIT', 'CREDIT')
                AND trans_timestamp <= CURRENT_TIMESTAMP()
           ) sub
           WHERE rn = 1
       ) s
       ON t.trans_id = s.trans_id
       WHEN NOT MATCHED THEN
       INSERT (
            trans_id,
            trans_timestamp,
            acct_number,
            customer_id,
            trans_type,
            amount,
            currency,
            sender_bank,
            receiver_bank,
            status,
            load_date
        )
        VALUES (
            s.trans_id,
            s.trans_timestamp,
            s.acct_number,
            s.customer_id,
            s.trans_type,
            s.amount,
            s.currency,
            s.sender_bank,
            s.receiver_bank,
            s.status,
            s.load_date
        );
           """
    )

    move_failed = SQLExecuteQueryOperator(
        task_id='move_failed',
        conn_id='snowflake_default',
        sql="""
        MERGE INTO FAILED_Transactions t
        USING (
            SELECT *
            FROM RAW_Transactions
            WHERE
                processed_flag = FALSE
                AND (
                    trans_id IS NULL
                    OR amount <= 0
                    OR trans_type NOT IN ('DEBIT', 'CREDIT')
                    OR trans_timestamp > CURRENT_TIMESTAMP()
                )
        ) s
        ON t.trans_id = s.trans_id
        WHEN NOT MATCHED THEN
        INSERT (
            trans_id,
            trans_timestamp,
            acct_number,
            customer_id,
            trans_type,
            amount,
            currency,
            sender_bank,
            receiver_bank,
            status,
            load_date
        )
        VALUES (
            s.trans_id,
            s.trans_timestamp,
            s.acct_number,
            s.customer_id,
            s.trans_type,
            s.amount,
            s.currency,
            s.sender_bank,
            s.receiver_bank,
            s.status,
            s.load_date
        );
        """
    )


    def validate_and_log(**context):
        import time
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    
        start_time = context['dag_run'].start_date
  
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    

        query = """
            SELECT
                COUNT(*) AS total_count,
        SUM(CASE
                        WHEN trans_id IS NOT NULL
                        AND amount > 0
                        AND trans_type IN ('DEBIT','CREDIT')
                        AND trans_timestamp <= CURRENT_TIMESTAMP()
                   THEN 1 ELSE 0 END) AS passed_count,
                SUM(CASE
                        WHEN trans_id IS NULL
                        OR amount <= 0
                        OR trans_type NOT IN ('DEBIT','CREDIT')
                        OR trans_timestamp > CURRENT_TIMESTAMP()
                  THEN 1 ELSE 0 END) AS failed_count
            FROM RAW_Transactions
            WHERE processed_flag = FALSE;
          """

        result = hook.get_first(query)

        total = result[0] or 0
        failed = result[2] or 0
        passed = result[1] or 0

        
        failure_percentage = (failed / total) * 100 if total > 0 else 0

        end_time = time.time()
        execution_time = round(end_time - context['ti'].start_date.timestamp(), 2)  

        if failure_percentage > 25:
            validation_status = 'CRITICAL'
            error_category = 'DATA_QUALITY'
        elif failure_percentage > 5:
            validation_status = 'FAILED'
            error_category = 'DATA_QUALITY'
        else:
            validation_status = 'SUCCESS'
            error_category = None

        insert_log_query = f"""
        INSERT INTO Pipeline_Run_Log (
                dag_run_id,
                run_timestamp,
                total_record,
                passed_records,
                failed_records,
                validation_status,
                error_category,
                execution_time_seconds
            )
            VALUES (
                '{context["run_id"]}',
                CURRENT_TIMESTAMP(),
                {total},
                {passed},
                {failed},
                '{validation_status}',
                {f"'{error_category}'" if error_category else "NULL"},
                {execution_time}
            );
            """
        hook.run(insert_log_query)

        print(f"Failure Percentage: {failure_percentage}%")
        print(f"Execution Time: {execution_time} seconds")

        if failure_percentage > 25:
            raise ValueError("Failure percentage exceeded more than 25% threshold!")

    validate_log = PythonOperator(
        task_id='validate_and_log',
        python_callable=validate_and_log,
    )

    def run_gx_checkpoint_and_log(**context):
        import subprocess
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')

        try:
            subprocess.run(
                ["great_expectations", "checkpoint", "run", "clean_transactions_checkpoint"],
                check=True
            )
            gx_status = 'SUCCESS'
            gx_error = None

        except subprocess.CalledProcessError as e:
            gx_status = 'FAILED'
            gx_error = str(e)

        gx_error_escaped = gx_error.replace("'", "''") if gx_error else None

        update_query = f"""
            UPDATE Pipeline_Run_Log
            SET gx_status = '{gx_status}',
                gx_error = {f"'{gx_error_escaped}'" if gx_error_escaped else "NULL"}
            WHERE dag_run_id = '{context["run_id"]}';
        """
        hook.run(update_query)
        print(f"GX checkpoint status logged: {gx_status}")

    gx_checkpoint = PythonOperator(
        task_id='gx_checkpoint',
        python_callable=run_gx_checkpoint_and_log
    )

    mark_processed = SQLExecuteQueryOperator(
        task_id='mark_processed',
        conn_id='snowflake_default',
        sql="""

        UPDATE RAW_Transactions
             SET processed_flag = TRUE
             WHERE processed_flag = FALSE;
        """
    )



    load_raw >> clean_data >> move_failed >> validate_log  >> mark_processed  >> gx_checkpoint
