import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path


COUNTRY_CURRENCY_MAP = {
    "US": "USD",
    "CA": "CAD",
    "GB": "GBP",
    "IN": "INR",
    "DE": "EUR",
}

VALID_MERCHANT_CATEGORIES = {
    "grocery",
    "restaurant",
    "travel",
    "electronics",
    "fuel",
    "entertainment",
    "gift_cards",
}

VALID_PAYMENT_METHODS = {
    "credit_card",
    "debit_card",
    "digital_wallet",
    "bank_transfer",
}

EXPECTED_COLUMNS = [
    "transaction_id",
    "event_timestamp",
    "customer_id",
    "merchant_id",
    "amount",
    "currency",
    "country",
    "merchant_category",
    "payment_method",
    "device_id",
    "is_fraud",
]


def add_error(errors: list[str], message: str, max_errors: int = 20) -> None:
    if len(errors) < max_errors:
        errors.append(message)


def validate_file(input_path: Path) -> bool:
    errors: list[str] = []
    seen_transaction_ids: set[str] = set()

    row_count = 0
    fraud_count = 0

    with input_path.open("r", newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)

        if reader.fieldnames != EXPECTED_COLUMNS:
            print("Validation failed: schema mismatch.")
            print(f"Expected: {EXPECTED_COLUMNS}")
            print(f"Actual:   {reader.fieldnames}")
            return False

        for line_number, row in enumerate(reader, start=2):
            row_count += 1

            transaction_id = row["transaction_id"]

            if not transaction_id:
                add_error(
                    errors,
                    f"Line {line_number}: transaction_id is empty.",
                )
            elif transaction_id in seen_transaction_ids:
                add_error(
                    errors,
                    f"Line {line_number}: duplicate transaction_id "
                    f"{transaction_id}.",
                )
            else:
                seen_transaction_ids.add(transaction_id)

            try:
                event_timestamp = datetime.fromisoformat(
                    row["event_timestamp"]
                )

                if event_timestamp.tzinfo is None:
                    add_error(
                        errors,
                        f"Line {line_number}: event_timestamp "
                        "must include timezone information.",
                    )
            except ValueError:
                add_error(
                    errors,
                    f"Line {line_number}: invalid event_timestamp "
                    f"{row['event_timestamp']}.",
                )

            try:
                amount = float(row["amount"])

                if amount <= 0:
                    add_error(
                        errors,
                        f"Line {line_number}: amount must be greater than zero.",
                    )
            except ValueError:
                add_error(
                    errors,
                    f"Line {line_number}: invalid amount {row['amount']}.",
                )

            country = row["country"]
            currency = row["currency"]

            if country not in COUNTRY_CURRENCY_MAP:
                add_error(
                    errors,
                    f"Line {line_number}: invalid country {country}.",
                )
            elif COUNTRY_CURRENCY_MAP[country] != currency:
                add_error(
                    errors,
                    f"Line {line_number}: country {country} "
                    f"does not match currency {currency}.",
                )

            if row["merchant_category"] not in VALID_MERCHANT_CATEGORIES:
                add_error(
                    errors,
                    f"Line {line_number}: invalid merchant_category "
                    f"{row['merchant_category']}.",
                )

            if row["payment_method"] not in VALID_PAYMENT_METHODS:
                add_error(
                    errors,
                    f"Line {line_number}: invalid payment_method "
                    f"{row['payment_method']}.",
                )

            if row["is_fraud"] not in {"True", "False"}:
                add_error(
                    errors,
                    f"Line {line_number}: is_fraud must be True or False.",
                )
            elif row["is_fraud"] == "True":
                fraud_count += 1

    if row_count == 0:
        errors.append("File contains no transaction rows.")

    print(f"Input file: {input_path}")
    print(f"Validated rows: {row_count:,}")
    print(f"Unique transaction IDs: {len(seen_transaction_ids):,}")
    print(f"Fraud rows: {fraud_count:,}")

    if row_count > 0:
        print(f"Fraud rate: {fraud_count / row_count:.2%}")

    if errors:
        print()
        print("VALIDATION FAILED")
        print(f"Errors found: {len(errors)}")

        for error in errors:
            print(f"- {error}")

        return False

    print()
    print("VALIDATION PASSED")
    return True


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate synthetic financial transaction CSV data."
    )

    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Path to the transaction CSV file.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    if not args.input.exists():
        print(f"Input file does not exist: {args.input}")
        sys.exit(1)

    if not args.input.is_file():
        print(f"Input path is not a file: {args.input}")
        sys.exit(1)

    validation_passed = validate_file(args.input)

    if not validation_passed:
        sys.exit(1)


if __name__ == "__main__":
    main()