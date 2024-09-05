import logging
from datetime import datetime
import pandas as pd

from scripts.s3_functions import upload_data_to_s3, fetch_all_files_from_s3
# from s3_functions import upload_data_to_s3, fetch_all_files_from_s3


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def late_payment_aggregated_view_task(destination_path:str, aggregated_view_path: str):
    try:
        billing_df = fetch_all_files_from_s3(destination_path, "billing")
        billing_df["updated_at"] = pd.to_datetime(billing_df["updated_at"])
        billing_df = billing_df.sort_values(by="updated_at").drop_duplicates(
            subset=["billing_id"], keep="last"
        )

        customer_information_df = fetch_all_files_from_s3(
            destination_path,
            "customer_information"
        )
        customer_information_df["updated_at"] = pd.to_datetime(
            customer_information_df["updated_at"]
        )
        customer_information_df = customer_information_df.sort_values(
            by="updated_at"
        ).drop_duplicates(subset=["customer_id"], keep="last")

        # Merge billing data with customer information
        billing_customer = pd.merge(
            billing_df, customer_information_df, on="customer_id"
        )
        # Create a new column to indicate whether the payment was on time
        billing_customer["on_time"] = (
            billing_customer["payment_date"] <= billing_customer["due_date"]
        )

        # Filter for late payments
        late_payments = billing_customer[billing_customer["on_time"] == False]

        # Count the number of late payments per customer
        late_payment_counts = (
            late_payments.groupby(["customer_id", "value_segment", "connection_type"])
            .size()
            .reset_index(name="late_payment_count")
        )

        # Sort by the number of late payments
        late_payment_counts_sorted = late_payment_counts.sort_values(
            by="late_payment_count", ascending=False
        )

        logger.info(
            late_payment_counts_sorted.head(10)
        )  # Display top 10 customers with the most late payments

        # Define the current date
        current_date = datetime.now()

        # Upload results to S3
        upload_data_to_s3(
            aggregated_view_path,
            "late_payment_analyses",
            late_payment_counts_sorted,
            current_date,
        )

    except Exception as error:
        logger.error(
            f"Billing data is not available, so average bill amount fact analyses can't be performed"
        )


def billing_amount_aggregated_view_task(destination_path:str, s3_key: str):
    try:
        billing_df = fetch_all_files_from_s3(destination_path, "billing")
        billing_df["updated_at"] = pd.to_datetime(billing_df["updated_at"])
        billing_df = billing_df.sort_values(by="updated_at").drop_duplicates(
            subset=["billing_id"], keep="last"
        )

        customer_information_df = fetch_all_files_from_s3(
            destination_path,
            "customer_information"
        )
        customer_information_df["updated_at"] = pd.to_datetime(
            customer_information_df["updated_at"]
        )
        customer_information_df = customer_information_df.sort_values(
            by="updated_at"
        ).drop_duplicates(subset=["customer_id"], keep="last")

        plans_df = fetch_all_files_from_s3(
            destination_path,
            "plans"
        )
        plans_df["updated_at"] = pd.to_datetime(
            plans_df["updated_at"]
        )
        plans_df = plans_df.sort_values(
            by="updated_at"
        ).drop_duplicates(subset=["tier"], keep="last")

        # Merge billing with customer information to get customer details
        billing_customer = pd.merge(
            billing_df, customer_information_df, on="customer_id"
        )

        # Merge with plans to get plan details
        billing_customer_plan = pd.merge(
            billing_customer, plans_df, left_on="value_segment", right_on="tier"
        )
        
        # Group by plan tier and calculate average billing amount
        average_billing_by_tier = billing_customer_plan.groupby('tier')['bill_amount'].mean().reset_index()
        average_billing_by_tier.columns = ['Plan Tier', 'Average Bill Amount']

        # Define the current date
        current_date = datetime.now()

        # Upload results to S3
        upload_data_to_s3(
            s3_key,
            "billing_amount_analysis",
            average_billing_by_tier,
            current_date,
        )

    except Exception as error:
        logger.error(error)