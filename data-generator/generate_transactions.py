import argparse
import csv
import random
import uuid
from datetime import date, datetime, time, timedelta, timezone
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
TRANSACTION_NAMESPACE = uuid.UUID("b68dbb32-b9d8-4d3a-a38d-13f956f71f9e")


def generate_transaction(
    rng: random.Random,
    transaction_index: int,
    seed: int,
    start_date: date,
    event_dates: int,
) -> dict:
    country = rng.choice(list(COUNTRIES.keys()))
    currency = COUNTRIES[country]

    amount = round(
        min(rng.lognormvariate(3.5, 1.0), 5000),
        2,
    )

    merchant_category = rng.choice(MERCHANT_CATEGORIES)
    payment_method = rng.choice(PAYMENT_METHODS)

    fraud_probability = 0.01

    if amount > 1000:
        fraud_probability += 0.12

    if merchant_category in {"electronics", "gift_cards"}:
        fraud_probability += 0.05

    if payment_method == "bank_transfer":
        fraud_probability += 0.03

    is_fraud = rng.random() < fraud_probability

    event_timestamp = datetime.combine(
        start_date + timedelta(days=rng.randrange(event_dates)),
        time.min,
        tzinfo=timezone.utc,
    ) + timedelta(
        seconds=rng.randrange(24 * 60 * 60)
    )

    return {
        "transaction_id": str(
            uuid.uuid5(
                TRANSACTION_NAMESPACE,
                f"seed={seed};row={transaction_index}",
            )
        ),
        "event_timestamp": event_timestamp.isoformat(),
        "customer_id": f"CUST_{rng.randint(1, 500):06d}",
        "merchant_id": f"MERCH_{rng.randint(1, 100):06d}",
        "amount": amount,
        "currency": currency,
        "country": country,
        "merchant_category": merchant_category,
        "payment_method": payment_method,
        "device_id": f"DEV_{rng.randint(1, 750):06d}",
        "is_fraud": is_fraud,
    }


def generate_dataset(
    row_count: int,
    output_dir: Path,
    seed: int,
    start_date: date,
    event_dates: int,
    output_name: str | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / (
        output_name or f"transactions_{row_count}_seed_{seed}.csv"
    )
    rng = random.Random(seed)

    fraud_count = 0

    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=FIELDNAMES)
        writer.writeheader()

        for transaction_index in range(row_count):
            transaction = generate_transaction(
                rng=rng,
                transaction_index=transaction_index,
                seed=seed,
                start_date=start_date,
                event_dates=event_dates,
            )
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
        "--start-date",
        type=date.fromisoformat,
        default=date(2026, 1, 1),
        help="First YYYY-MM-DD date represented in the generated data.",
    )

    parser.add_argument(
        "--event-dates",
        type=int,
        default=31,
        help="Number of consecutive event dates represented.",
    )

    parser.add_argument(
        "--output-name",
        default=None,
        help="Optional deterministic CSV filename.",
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

    if args.event_dates <= 0:
        raise ValueError("--event-dates must be greater than zero.")

    generate_dataset(
        row_count=args.rows,
        output_dir=args.output_dir,
        seed=args.seed,
        start_date=args.start_date,
        event_dates=args.event_dates,
        output_name=args.output_name,
    )


if __name__ == "__main__":
    main()
