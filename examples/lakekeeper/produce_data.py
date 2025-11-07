import psycopg2
import random
import time
from faker import Faker

# --- Database Connection Details ---
# Based on your Debezium configuration, the host/port/user/password for the *source* DB
DB_HOST = "localhost"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_PORT = 5432

# --- Configuration ---
INSERT_INTERVAL_SECONDS = 10
fake = Faker()


def get_db_connection():
    """Establishes and returns a PostgreSQL database connection."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        return conn
    except Exception as error:
        print(f"❌ Error connecting to the database: {error}")
        return None


def insert_random_customer(conn):
    first_name = f"test-{fake.first_name()}"
    last_name = fake.last_name()
    email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 100)}@{fake.domain_name()}"

    insert_query = """
    INSERT INTO inventory.customers (first_name, last_name, email)
    VALUES (%s, %s, %s)
    RETURNING id;
    """

    try:
        with conn.cursor() as cur:
            cur.execute(insert_query, (first_name, last_name, email))
            new_id = cur.fetchone()[0]
            conn.commit()
            print(f"✅ Inserted new customer (ID: {new_id}): {first_name} {last_name}")
            return new_id
    except Exception as error:
        print(f"❌ Error inserting customer data: {error}")
        conn.rollback()
        raise error


def insert_random_order(conn, purchaser_id):
    """Inserts a new random order into the inventory.orders table."""
    # Ensure there's at least one customer ID to reference
    order_date = fake.date_time_this_year()
    quantity = random.randint(1, 10)
    product_id = random.randint(101, 109)  # Assumes existing products

    insert_query = """
    INSERT INTO inventory.orders (order_date, purchaser, quantity, product_id)
    VALUES (%s, %s, %s, %s);
    """

    try:
        with conn.cursor() as cur:
            cur.execute(insert_query, (order_date, purchaser_id, quantity, product_id))
            conn.commit()
            print(f"✅ Inserted new order for purchaser ID {purchaser_id} (Product: {product_id}, Quantity: {quantity})")
    except Exception as error:
        print(f"❌ Error inserting order data: {error}")
        conn.rollback()


def main():
    """Main function to run the continuous insertion loop."""
    print("🚀 Starting continuous data insertion script...")

    conn = get_db_connection()
    try:
        while True:
            print("-" * 30)
            purchaser_id = insert_random_customer(conn)
            num_orders = random.randint(0, 3)
            for _ in range(num_orders):
                insert_random_order(conn, purchaser_id)

            print(f"⏳ Waiting for {INSERT_INTERVAL_SECONDS} seconds...")
            time.sleep(INSERT_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\n\n🛑 Script stopped by user (Ctrl+C).")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()
