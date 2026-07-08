import argparse
import csv
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path


COUNTRIES = {
    "US": "USD",
    "CA": "CAD",
    "GB": "GBP",
    "IN": "INR",
    "DE": "EUR",
}

MERCHANT_CATEGORIES = [
    "grocery",
    "restaurant",
    "travel",
    "electronics",
    "fuel",
    "entertainment",
    "gift_cards",
]

PAYMENT_METHODS = [
    "credit_card",
    "debit_card",
    "digital_wallet",
    "bank_transfer",
]

FIELDNAMES = [
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


def generate_transaction() -> dict:
    country = random.choice(list(COUNTRIES.keys()))
    currency = COUNTRIES[country]

    amount = round(
        min(random.lognormvariate(3.5, 1.0), 5000),
        2,
    )

    merchant_category = random.choice(MERCHANT_CATEGORIES)
    payment_method = random.choice(PAYMENT_METHODS)

    fraud_probability = 0.01

    if amount > 1000:
        fraud_probability += 0.12

    if merchant_category in {"electronics", "gift_cards"}:
        fraud_probability += 0.05

    if payment_method == "bank_transfer":
        fraud_probability += 0.03

    is_fraud = random.random() < fraud_probability

    event_timestamp = datetime.now(timezone.utc) - timedelta(
        seconds=random.randint(0, 30 * 24 * 60 * 60)
    )

    return {
        "transaction_id": str(uuid.uuid4()),
        "event_timestamp": event_timestamp.isoformat(),
        "customer_id": f"CUST_{random.randint(1, 500):06d}",
        "merchant_id": f"MERCH_{random.randint(1, 100):06d}",
        "amount": amount,
        "currency": currency,
        "country": country,
        "merchant_category": merchant_category,
        "payment_method": payment_method,
        "device_id": f"DEV_{random.randint(1, 750):06d}",
        "is_fraud": is_fraud,
    }


def generate_dataset(row_count: int, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    output_path = output_dir / f"transactions_{timestamp}.csv"

    fraud_count = 0

    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=FIELDNAMES)
        writer.writeheader()

        for _ in range(row_count):
            transaction = generate_transaction()
            writer.writerow(transaction)

            if transaction["is_fraud"]:
                fraud_count += 1

    fraud_rate = fraud_count / row_count if row_count else 0

    print(f"Generated rows: {row_count:,}")
    print(f"Fraud rows: {fraud_count:,}")
    print(f"Fraud rate: {fraud_rate:.2%}")
    print(f"Output file: {output_path}")
    print(f"File size: {output_path.stat().st_size:,} bytes")

    return output_path


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic financial transaction data."
    )

    parser.add_argument(
        "--rows",
        type=int,
        default=100,
        help="Number of transactions to generate.",
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/raw/transactions"),
        help="Directory where generated CSV files are stored.",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible data generation.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    if args.rows <= 0:
        raise ValueError("--rows must be greater than zero.")

    random.seed(args.seed)

    generate_dataset(
        row_count=args.rows,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main()