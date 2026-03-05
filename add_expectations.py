import great_expectations as gx
import datetime
from great_expectations.core.batch import RuntimeBatchRequest

context = gx.get_context()

suite = context.get_expectation_suite("clean_transactions_suite")

batch_request = RuntimeBatchRequest(
    datasource_name="snowflake_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="clean_transactions",
    runtime_parameters={
        "query": "SELECT * FROM clean_transactions"
    },
    batch_identifiers={
        "default_identifier_name": "default_id"
    }
)

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="clean_transactions_suite",
)


validator.expect_column_values_to_not_be_null("trans_id")
validator.expect_column_values_to_be_unique("trans_id")


validator.expect_column_values_to_not_be_null("trans_timestamp")
validator.expect_column_max_to_be_between("trans_timestamp", min_value=None, max_value=datetime.datetime.now())

validator.expect_column_values_to_not_be_null("acct_number")

validator.expect_column_values_to_not_be_null("customer_id")


validator.expect_column_values_to_not_be_null("amount")
validator.expect_column_values_to_be_between("amount", min_value=0, max_value=1000000)


validator.expect_column_values_to_be_in_set("currency", ["INR"])


validator.expect_column_values_to_be_in_set("trans_type", ["DEBIT", "CREDIT"])


validator.expect_column_values_to_be_in_set("status", ["SUCCESS", "FAILED"])

validator.expect_column_values_to_be_in_set("sender_bank", ["HDFC", "AXIS", "ICICI", "SBI", "HSBC", "Barclays"])

validator.expect_column_values_to_be_in_set("receiver_bank", ["HDFC", "AXIS", "ICICI", "SBI", "HSBC", "Barclays"])

validator.expect_column_values_to_not_be_null("load_date")

validator.save_expectation_suite(discard_failed_expectations=False)
