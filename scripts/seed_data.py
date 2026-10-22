
import csv
import io
import os
import random
import time
from datetime import datetime, timedelta, timezone

import psycopg
from dotenv import load_dotenv

load_dotenv()

MERCHANT_COUNT = 2_000
TRANSACTION_COUNT = 5_000_000
BATCH_SIZE = 50_000

PROVINCES = [
    "Tehran", "Isfahan", "Fars", "Gilan",
    "Khorasan", "Yazd", "Alborz"
]
MCCS = ["5411", "5812", "5732", "5999", "5541"]
RISK_LEVELS = ["LOW", "MEDIUM", "HIGH"]
CHANNELS = ["POS", "IPG", "QR"]
CARD_BINS = ["603799", "589210", "627412", "621986"]


def get_connection():
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        dbname=os.getenv("POSTGRES_DB", "payment_db"),
        user=os.getenv("POSTGRES_USER", "zibal"),
        password=os.environ["POSTGRES_PASSWORD"],
    )


def seed_merchants(conn):
    print("Seeding merchants...")

    rows = []

    for merchant_id in range(1, MERCHANT_COUNT + 1):
        rows.append((
            merchant_id,
            f"Merchant {merchant_id}",
            random.choice(MCCS),
            random.choice(PROVINCES),
            random.choice(RISK_LEVELS),
            f"{merchant_id:010d}",
            f"0912{merchant_id:07d}",
        ))

    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO core.merchants (
                merchant_id, name, mcc, province,
                risk_level, owner_national_id,
                owner_mobile
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (merchant_id) DO NOTHING
            """,
            rows,
        )

    conn.commit()
    print("Merchants ready.")


def seed_transactions(conn):
    print("Seeding transactions...")
    start_time = time.monotonic()

    # Resume from last successfully committed batch.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(MAX(transaction_id), 0) "
            "FROM core.transactions"
        )
        last_id = cur.fetchone()[0]

    if last_id >= TRANSACTION_COUNT:
        print("Transactions already seeded.")
        return

    end_time = datetime.now(timezone.utc)
    start_date = end_time - timedelta(days=180)

    for batch_start in range(
        last_id + 1, TRANSACTION_COUNT + 1, BATCH_SIZE
    ):
        batch_end = min(
            batch_start + BATCH_SIZE - 1,
            TRANSACTION_COUNT
        )

        buffer = io.StringIO()
        writer = csv.writer(buffer)

        for transaction_id in range(batch_start, batch_end + 1):
            amount = random.randint(10_000, 10_000_000)

            # Approximately 80% success, 10% failed,
            # and 10% reversed.
            r = random.random()

            if r < 0.8:
                status = "SUCCESS"
            elif r < 0.9:
                status = "FAILED"
            else:
                status = "REVERSED"

            created_at = start_date + timedelta(
                seconds=random.random() * 180 * 86400
            )
            updated_at = created_at + timedelta(seconds=5)

            writer.writerow([
                transaction_id,
                random.randint(1, MERCHANT_COUNT),
                random.choice(CHANNELS),
                random.choice(CARD_BINS),
                amount,
                amount // 100,
                status,
                created_at.isoformat(),
                updated_at.isoformat(),
            ])

        buffer.seek(0)

        with conn.cursor() as cur:
            with cur.copy(
                """
                COPY core.transactions (
                    transaction_id,
                    merchant_id,
                    channel,
                    card_bin,
                    amount,
                    fee_amount,
                    status,
                    created_at,
                    updated_at
                )
                FROM STDIN WITH (FORMAT CSV)
                """
            ) as copy:
                copy.write(buffer.getvalue())

        conn.commit()

        elapsed = time.monotonic() - start_time
        print(
            f"Inserted through ID {batch_end:,} / "
            f"{TRANSACTION_COUNT:,} | {elapsed:.1f}s",
            flush=True,
        )

    with conn.cursor() as cur:
        cur.execute("ANALYZE core.merchants")
        cur.execute("ANALYZE core.transactions")

    conn.commit()
    print("Transaction seeding completed.")


def validate(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM core.merchants")
        merchants = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM core.transactions")
        transactions = cur.fetchone()[0]

        cur.execute(
            """
            SELECT status, COUNT(*)
            FROM core.transactions
            GROUP BY status
            ORDER BY status
            """
        )
        statuses = cur.fetchall()

    print(f"\nMerchants: {merchants:,}")
    print(f"Transactions: {transactions:,}")

    for status, count in statuses:
        print(
            f"{status}: {count:,} "
            f"({count / transactions * 100:.2f}%)"
        )

    assert merchants >= MERCHANT_COUNT
    assert transactions >= TRANSACTION_COUNT


def main():
    random.seed(42)

    with get_connection() as conn:
        seed_merchants(conn)
        seed_transactions(conn)
        validate(conn)

    print("\nInitial data generation successful!")


if __name__ == "__main__":
    main()
