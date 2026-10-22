
import argparse
import os
import random
import time
from collections import deque
from datetime import datetime, timezone

import psycopg
from dotenv import load_dotenv

load_dotenv()

TRANSACTIONS_PER_SECOND = 20
SETTLEMENT_DELAY = 5
REVERSAL_DELAY = 20
DELETE_DELAY = 12

PROVINCES = [
    "Tehran", "Isfahan", "Fars", "Gilan",
    "Khorasan", "Yazd", "Alborz"
]
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
        autocommit=True,
    )


def insert_transactions(cur, next_id, merchant_ids):
    now = datetime.now(timezone.utc)
    rows = []

    for i in range(TRANSACTIONS_PER_SECOND):
        amount = random.randint(10_000, 10_000_000)

        rows.append((
            next_id + i,
            random.choice(merchant_ids),
            random.choice(CHANNELS),
            random.choice(CARD_BINS),
            amount,
            amount // 100,
            now,
            now,
        ))

    cur.executemany(
        """
        INSERT INTO core.transactions (
            transaction_id, merchant_id, channel,
            card_bin, amount, fee_amount,
            status, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s,
                'PENDING', %s, %s)
        """,
        rows,
    )

    return [row[0] for row in rows]


def settle_transactions(cur, ids):
    success_ids = []
    failed_ids = []

    for transaction_id in ids:
        if random.random() < 0.9:
            success_ids.append(transaction_id)
        else:
            failed_ids.append(transaction_id)

    for status, selected_ids in (
        ("SUCCESS", success_ids),
        ("FAILED", failed_ids),
    ):
        if selected_ids:
            cur.execute(
                """
                UPDATE core.transactions
                SET status = %s,
                    updated_at = NOW()
                WHERE transaction_id = ANY(%s)
                  AND status = 'PENDING'
                """,
                (status, selected_ids),
            )

    # Approximately 10% of successful transactions
    # will later become REVERSED.
    reversal_ids = [
        transaction_id
        for transaction_id in success_ids
        if random.random() < 0.10
    ]

    return reversal_ids


def reverse_transactions(cur, ids):
    if not ids:
        return

    cur.execute(
        """
        UPDATE core.transactions
        SET status = 'REVERSED',
            updated_at = NOW()
        WHERE transaction_id = ANY(%s)
          AND status = 'SUCCESS'
        """,
        (ids,),
    )


def update_merchant(cur, merchant_ids):
    merchant_id = random.choice(merchant_ids)

    cur.execute(
        """
        SELECT risk_level, province
        FROM core.merchants
        WHERE merchant_id = %s
        """,
        (merchant_id,),
    )
    old_risk, old_province = cur.fetchone()

    new_risk = random.choice(
        [x for x in RISK_LEVELS if x != old_risk]
    )
    new_province = random.choice(
        [x for x in PROVINCES if x != old_province]
    )

    cur.execute(
        """
        UPDATE core.merchants
        SET risk_level = %s,
            province = %s,
            updated_at = NOW()
        WHERE merchant_id = %s
        """,
        (new_risk, new_province, merchant_id),
    )

    print(
        f"Merchant {merchant_id}: "
        f"risk {old_risk}->{new_risk}, "
        f"province {old_province}->{new_province}"
    )


def insert_test_transaction(cur, transaction_id, merchant_ids):
    now = datetime.now(timezone.utc)

    cur.execute(
        """
        INSERT INTO core.transactions (
            transaction_id, merchant_id, channel,
            card_bin, amount, fee_amount,
            status, created_at, updated_at
        )
        VALUES (%s, %s, 'QR', '603799',
                10000, 100, 'PENDING', %s, %s)
        """,
        (
            transaction_id,
            random.choice(merchant_ids),
            now,
            now,
        ),
    )


def delete_test_transaction(cur, transaction_id):
    cur.execute(
        """
        DELETE FROM core.transactions
        WHERE transaction_id = %s
        """,
        (transaction_id,),
    )


def run(duration):
    pending = deque()
    reversals = deque()
    deletions = deque()

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(transaction_id), 0) "
                "FROM core.transactions"
            )
            next_id = cur.fetchone()[0] + 1

            cur.execute(
                "SELECT merchant_id FROM core.merchants"
            )
            merchant_ids = [row[0] for row in cur.fetchall()]

        if not merchant_ids:
            raise RuntimeError(
                "No merchants found. Run seed_data.py first."
            )

        start = time.monotonic()
        cycle = 0

        print("Change simulator started. Press Ctrl+C to stop.")

        try:
            while True:
                tick_start = time.monotonic()
                elapsed = tick_start - start

                if duration and elapsed >= duration:
                    break

                cycle += 1

                # Each cycle is committed atomically.
                with conn.transaction():
                    with conn.cursor() as cur:
                        ids = insert_transactions(
                            cur, next_id, merchant_ids
                        )
                        next_id += len(ids)
                        pending.append((tick_start, ids))

                        # PENDING -> SUCCESS / FAILED
                        while (
                            pending
                            and tick_start - pending[0][0]
                            >= SETTLEMENT_DELAY
                        ):
                            _, old_ids = pending.popleft()
                            reverse_ids = settle_transactions(
                                cur, old_ids
                            )
                            reversals.append(
                                (tick_start, reverse_ids)
                            )

                        # SUCCESS -> REVERSED
                        while (
                            reversals
                            and tick_start - reversals[0][0]
                            >= REVERSAL_DELAY
                        ):
                            _, old_ids = reversals.popleft()
                            reverse_transactions(cur, old_ids)

                        # Merchant attribute changes
                        if cycle % 60 == 0:
                            update_merchant(cur, merchant_ids)

                        # Create a test transaction every 30 sec
                        if cycle % 30 == 0:
                            test_id = next_id
                            next_id += 1
                            insert_test_transaction(
                                cur, test_id, merchant_ids
                            )
                            deletions.append(
                                (tick_start, test_id)
                            )
                            print(f"Test INSERT: {test_id}")

                        # Hard delete test transactions
                        while (
                            deletions
                            and tick_start - deletions[0][0]
                            >= DELETE_DELAY
                        ):
                            _, test_id = deletions.popleft()
                            delete_test_transaction(cur, test_id)
                            print(f"Hard DELETE: {test_id}")

                if cycle % 10 == 0:
                    print(
                        f"Cycle {cycle}: "
                        f"{cycle * TRANSACTIONS_PER_SECOND:,} "
                        "regular transactions inserted",
                        flush=True,
                    )

                # Keep approximately one cycle per second.
                remaining = 1.0 - (
                    time.monotonic() - tick_start
                )
                if remaining > 0:
                    time.sleep(remaining)

        except KeyboardInterrupt:
            print("\nSimulator stopped.")

    print("Simulation finished.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--duration",
        type=int,
        default=0,
        help="Run duration in seconds; 0 means continuous.",
    )
    args = parser.parse_args()

    run(args.duration)
