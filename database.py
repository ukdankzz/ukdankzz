import os
import time
import json
import logging
import psycopg2
import psycopg2.extras
from psycopg2.extras import RealDictCursor
from psycopg2 import pool as pg_pool
from typing import List, Dict, Optional, Any, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseManager:
    """Database manager with resilient lazy-init connection pooling.

    Key fixes vs original:
    - Pool is created lazily on first use, not at __init__ time.
      This prevents Railway / Neon cold-start failures from crashing the
      whole process before the bot even starts.
    - Pool is recreated automatically when it becomes unusable (e.g. after
      the Neon endpoint is re-enabled).
    - put_connection no longer recurses infinitely when the pool is None.
    - Duplicate validate_coupon method removed; the correct signature kept.
    - export_orders conn.close() replaced with put_connection in main.py.
    """

    # ------------------------------------------------------------------ #
    #  Init & pool management                                              #
    # ------------------------------------------------------------------ #

    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable is required")
        self.connection_pool: Optional[pg_pool.ThreadedConnectionPool] = None
        self._pool_lock = __import__('threading').Lock()

    def _create_pool(self) -> bool:
        """Create (or recreate) the connection pool. Returns True on success."""
        try:
            new_pool = pg_pool.ThreadedConnectionPool(
                minconn=1,   # Keep only 1 idle conn so cold Neon endpoints
                maxconn=10,  # don't get bombarded with keepalive noise.
                dsn=self.database_url,
                cursor_factory=RealDictCursor,
                connect_timeout=15,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
            )
            self.connection_pool = new_pool
            logger.info("✅ Database connection pool initialised (1-10 connections)")
            return True
        except Exception as exc:
            logger.error(f"❌ Failed to create connection pool: {exc}")
            self.connection_pool = None
            return False

    def _get_pool(self) -> Optional[pg_pool.ThreadedConnectionPool]:
        """Return the pool, recreating it if it is closed or None."""
        with self._pool_lock:
            if self.connection_pool is None or self.connection_pool.closed:
                self._create_pool()
            return self.connection_pool

    # ------------------------------------------------------------------ #
    #  Core connection helpers                                             #
    # ------------------------------------------------------------------ #

    def get_connection(self, retries: int = 3):
        """Get a connection – from pool if possible, else direct."""
        pool = self._get_pool()
        if pool and not pool.closed:
            try:
                conn = pool.getconn()
                if conn and not conn.closed:
                    return conn
            except Exception as exc:
                logger.error(f"❌ Failed to get connection from pool: {exc}")
                # Pool may be broken – drop it so _get_pool rebuilds next time
                with self._pool_lock:
                    self.connection_pool = None

        # Fallback: direct connection with retries
        last_exc = None
        for attempt in range(retries):
            try:
                conn = psycopg2.connect(
                    self.database_url,
                    cursor_factory=RealDictCursor,
                    connect_timeout=15,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5,
                )
                logger.info(f"✅ Direct DB connection established (attempt {attempt + 1})")
                return conn
            except Exception as exc:
                last_exc = exc
                logger.error(f"❌ Direct connection attempt {attempt + 1}/{retries} failed: {exc}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)

        logger.error("❌ All database connection attempts failed")
        raise last_exc or psycopg2.OperationalError("Failed to connect to database")

    def put_connection(self, conn):
        """Return connection to pool, or close it if pool is unavailable."""
        if conn is None:
            return
        pool = self.connection_pool
        if pool and not pool.closed:
            try:
                pool.putconn(conn)
                return
            except Exception as exc:
                logger.warning(f"⚠️ Failed to return connection to pool: {exc}")
        # Pool unavailable – just close the connection to avoid leaking it
        try:
            conn.close()
        except Exception:
            pass

    def health_check(self) -> bool:
        """Return True if the database is reachable."""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            logger.info("✅ Database health check passed")
            return True
        except Exception as exc:
            logger.error(f"❌ Database health check failed: {exc}")
            return False
        finally:
            if conn:
                self.put_connection(conn)

    # ------------------------------------------------------------------ #
    #  Table initialisation                                                #
    # ------------------------------------------------------------------ #

    def init_tables(self):
        """Create all required tables and indexes if they don't exist."""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS reviews (
                        id SERIAL PRIMARY KEY,
                        username VARCHAR(255) NOT NULL,
                        rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
                        review_text TEXT NOT NULL,
                        review_date DATE NOT NULL,
                        order_items JSONB,
                        order_num VARCHAR(50),
                        user_id BIGINT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS orders (
                        id SERIAL PRIMARY KEY,
                        order_num VARCHAR(50) NOT NULL UNIQUE,
                        user_id BIGINT NOT NULL,
                        username VARCHAR(255),
                        status VARCHAR(50) DEFAULT 'pending',
                        order_details JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        confirmed_at TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS order_confirmations (
                        id SERIAL PRIMARY KEY,
                        order_num VARCHAR(50) NOT NULL UNIQUE,
                        user_id BIGINT NOT NULL,
                        username VARCHAR(255),
                        order_details TEXT,
                        confirmed_by_admin BOOLEAN DEFAULT FALSE,
                        confirmation_date TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS promotions (
                        id SERIAL PRIMARY KEY,
                        type VARCHAR(20) NOT NULL CHECK (type IN ('bundle','item','collection','flash')),
                        name VARCHAR(255) NOT NULL,
                        description TEXT,
                        target_product_ids JSONB DEFAULT '[]',
                        percent_off NUMERIC(5,2) DEFAULT 0,
                        amount_off NUMERIC(10,2) DEFAULT 0,
                        buy_qty INTEGER DEFAULT 1,
                        get_qty INTEGER DEFAULT 0,
                        min_spend NUMERIC(10,2) DEFAULT 0,
                        start_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        end_at TIMESTAMP,
                        stackable BOOLEAN DEFAULT FALSE,
                        segments JSONB DEFAULT '[]',
                        active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS coupons (
                        code VARCHAR(50) PRIMARY KEY,
                        type VARCHAR(10) NOT NULL CHECK (type IN ('percent','fixed')),
                        value NUMERIC(10,2) NOT NULL,
                        min_spend NUMERIC(10,2) DEFAULT 0,
                        expires_at TIMESTAMP,
                        max_uses INTEGER DEFAULT 1000,
                        current_uses INTEGER DEFAULT 0,
                        per_user_limit INTEGER DEFAULT 1,
                        active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS coupon_usage (
                        id SERIAL PRIMARY KEY,
                        coupon_code VARCHAR(50) REFERENCES coupons(code) ON DELETE CASCADE,
                        user_id BIGINT NOT NULL,
                        order_num VARCHAR(50),
                        used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS bot_instance (
                        id SERIAL PRIMARY KEY,
                        instance_id VARCHAR(255) UNIQUE NOT NULL,
                        lease_until TIMESTAMP NOT NULL,
                        heartbeat_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        host_info VARCHAR(500),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS broadcast_users (
                        user_id BIGINT PRIMARY KEY,
                        username VARCHAR(255),
                        first_name VARCHAR(255),
                        last_name VARCHAR(255),
                        first_interaction TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS blocked_users (
                        user_id BIGINT PRIMARY KEY,
                        username VARCHAR(255),
                        blocked_by VARCHAR(255),
                        reason TEXT,
                        blocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS menu_categories (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(100) NOT NULL UNIQUE,
                        display_order INTEGER DEFAULT 0,
                        active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS menu_products (
                        id SERIAL PRIMARY KEY,
                        category_id INTEGER REFERENCES menu_categories(id) ON DELETE CASCADE,
                        name VARCHAR(200) NOT NULL,
                        description TEXT,
                        active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(category_id, name)
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS menu_pricing (
                        id SERIAL PRIMARY KEY,
                        product_id INTEGER REFERENCES menu_products(id) ON DELETE CASCADE,
                        size VARCHAR(50) NOT NULL,
                        price NUMERIC(10,2) NOT NULL,
                        display_order INTEGER DEFAULT 0,
                        active BOOLEAN DEFAULT TRUE
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS freebie_claims (
                        id SERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL UNIQUE,
                        username VARCHAR(255),
                        claimed_product VARCHAR(255),
                        claimed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)

                # Indexes
                for stmt in [
                    "CREATE INDEX IF NOT EXISTS idx_reviews_user_id ON reviews(user_id)",
                    "CREATE INDEX IF NOT EXISTS idx_promotions_active ON promotions(active, start_at, end_at)",
                    "CREATE INDEX IF NOT EXISTS idx_coupons_active ON coupons(active, expires_at)",
                    "CREATE INDEX IF NOT EXISTS idx_coupon_usage_user ON coupon_usage(user_id, coupon_code)",
                    "CREATE INDEX IF NOT EXISTS idx_reviews_order_num ON reviews(order_num)",
                    "CREATE INDEX IF NOT EXISTS idx_reviews_date ON reviews(review_date)",
                    "CREATE INDEX IF NOT EXISTS idx_orders_user_id ON order_confirmations(user_id)",
                    "CREATE INDEX IF NOT EXISTS idx_orders_order_num ON order_confirmations(order_num)",
                    "CREATE INDEX IF NOT EXISTS idx_bot_instance_lease ON bot_instance(instance_id, lease_until)",
                    "CREATE INDEX IF NOT EXISTS idx_broadcast_users_active ON broadcast_users(active, last_seen)",
                    "CREATE INDEX IF NOT EXISTS idx_blocked_users ON blocked_users(user_id)",
                    "CREATE INDEX IF NOT EXISTS idx_menu_categories_order ON menu_categories(display_order, active)",
                    "CREATE INDEX IF NOT EXISTS idx_menu_products_category ON menu_products(category_id, active)",
                    "CREATE INDEX IF NOT EXISTS idx_menu_pricing_product ON menu_pricing(product_id, display_order)",
                    "CREATE INDEX IF NOT EXISTS idx_freebie_claims_user ON freebie_claims(user_id)",
                ]:
                    cur.execute(stmt)

                conn.commit()
                logger.info("✅ Database tables initialised successfully")
        except Exception as exc:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.error(f"❌ Error initialising database tables: {exc}")
            raise
        finally:
            if conn:
                self.put_connection(conn)

    # ------------------------------------------------------------------ #
    #  Reviews                                                             #
    # ------------------------------------------------------------------ #

    def save_review(self, username, rating, review_text,
                    review_date, order_items, order_num, user_id) -> bool:
        try:
            username = str(username).strip()[:255]
            if not username:
                return False
            if not isinstance(rating, int) or not (1 <= rating <= 5):
                return False
            review_text = str(review_text).strip()[:2000]
            if len(review_text) < 3:
                return False
            order_num = str(order_num).strip()[:50]
            if not order_num:
                return False
            if not isinstance(user_id, int) or user_id <= 0:
                return False
            if not isinstance(order_items, list):
                order_items = []
            else:
                order_items = [
                    {
                        'name': str(i.get('name', ''))[:100],
                        'size': str(i.get('size', ''))[:50],
                        'price': float(i.get('price', 0)) if isinstance(i.get('price'), (int, float)) else 0,
                    }
                    for i in order_items[:20] if isinstance(i, dict)
                ]
        except Exception as exc:
            logger.error(f"❌ Review input validation error: {exc}")
            return False

        for attempt in range(3):
            conn = None
            try:
                conn = self.get_connection()
                with conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO reviews
                                (username, rating, review_text, review_date, order_items, order_num, user_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (username, rating, review_text, review_date,
                              psycopg2.extras.Json(order_items), order_num, user_id))
                logger.info(f"✅ Review saved for user {user_id}, order {order_num}")
                return True
            except psycopg2.IntegrityError:
                return False
            except Exception as exc:
                logger.error(f"❌ Error saving review (attempt {attempt + 1}/3): {exc}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
            finally:
                if conn:
                    self.put_connection(conn)
        return False

    def get_all_reviews(self, limit: int = 50, offset: int = 0) -> List[Dict]:
        limit = max(1, min(int(limit), 1000))
        offset = max(0, int(offset))
        for attempt in range(3):
            conn = None
            try:
                conn = self.get_connection()
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT username, rating, review_text, review_date,
                               order_items, order_num, user_id
                        FROM reviews
                        ORDER BY created_at DESC
                        LIMIT %s OFFSET %s
                    """, (limit, offset))
                    rows = cur.fetchall()
                result = []
                for row in rows:
                    try:
                        d = dict(row)
                        result.append({
                            'username': str(d.get('username', 'Unknown'))[:255],
                            'rating': max(1, min(5, int(d.get('rating', 5)))),
                            'review_text': str(d.get('review_text', ''))[:2000],
                            'review_date': d.get('review_date'),
                            'order_items': d.get('order_items') or [],
                            'order_num': str(d.get('order_num', 'N/A'))[:50],
                            'user_id': d.get('user_id', 0),
                        })
                    except Exception:
                        continue
                return result
            except Exception as exc:
                logger.error(f"❌ Error getting reviews (attempt {attempt + 1}/3): {exc}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
            finally:
                if conn:
                    self.put_connection(conn)
        return []

    def user_has_reviewed_order(self, user_id: int, order_num: str) -> bool:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) AS cnt FROM reviews WHERE user_id = %s AND order_num = %s",
                    (user_id, order_num)
                )
                row = cur.fetchone()
                return (row['cnt'] if isinstance(row, dict) else row[0]) > 0
        except Exception as exc:
            logger.error(f"❌ Error checking review: {exc}")
            return False
        finally:
            if conn:
                self.put_connection(conn)

    def get_review_count(self) -> int:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS cnt FROM reviews")
                row = cur.fetchone()
                return row['cnt'] if isinstance(row, dict) else row[0]
        except Exception as exc:
            logger.error(f"❌ Error getting review count: {exc}")
            return 0
        finally:
            if conn:
                self.put_connection(conn)

    def get_review_statistics(self) -> Dict:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) AS total_reviews,
                           COALESCE(AVG(rating), 0) AS average_rating
                    FROM reviews
                """)
                row = cur.fetchone()
                if row:
                    if isinstance(row, dict):
                        return {
                            'total_reviews': row.get('total_reviews', 0),
                            'average_rating': float(row.get('average_rating', 0)),
                        }
                    return {'total_reviews': row[0] or 0, 'average_rating': float(row[1] or 0)}
            return {'total_reviews': 0, 'average_rating': 0.0}
        except Exception as exc:
            logger.error(f"❌ Error getting review statistics: {exc}")
            return {'total_reviews': 0, 'average_rating': 0.0}
        finally:
            if conn:
                self.put_connection(conn)

    def get_reviews_for_product(self, product_name: str, limit: int = 5) -> List[Dict]:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT username, rating, review_text, review_date,
                           order_items, order_num, user_id
                    FROM reviews
                    WHERE EXISTS (
                        SELECT 1 FROM jsonb_array_elements(order_items) AS elem
                        WHERE elem->>'name' = %s
                    )
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (product_name, limit))
                return [dict(r) for r in cur.fetchall()]
        except Exception as exc:
            logger.error(f"❌ Error getting reviews for {product_name}: {exc}")
            return []
        finally:
            if conn:
                self.put_connection(conn)

    def migrate_existing_reviews(self, existing_reviews: List[tuple]) -> int:
        migrated = 0
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                for review in existing_reviews:
                    try:
                        if len(review) >= 6:
                            username, rating, review_text, date, order_items, order_num = review[:6]
                            user_id = 0
                        else:
                            username, rating, review_text, date = review[:4]
                            order_items, order_num, user_id = [], "MIGRATED", 0
                        cur.execute("""
                            INSERT INTO reviews
                                (username, rating, review_text, review_date, order_items, order_num, user_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING
                        """, (username, rating, review_text, date,
                              psycopg2.extras.Json(order_items), order_num, user_id))
                        migrated += 1
                    except Exception as exc:
                        logger.error(f"❌ Error migrating review: {exc}")
            conn.commit()
            logger.info(f"✅ Migrated {migrated} reviews")
        except Exception as exc:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.error(f"❌ Migration error: {exc}")
        finally:
            if conn:
                self.put_connection(conn)
        return migrated

    # ------------------------------------------------------------------ #
    #  Orders                                                              #
    # ------------------------------------------------------------------ #

    def get_total_orders_count(self) -> int:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS cnt FROM orders")
                row = cur.fetchone()
                count = row['cnt'] if isinstance(row, dict) else row[0]
                return 106 + count
        except Exception as exc:
            logger.error(f"❌ Error getting total orders count: {exc}")
            return 106
        finally:
            if conn:
                self.put_connection(conn)

    def get_next_order_number(self) -> str:
        import random
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                for _ in range(50):
                    n = random.randint(10000, 99999)
                    order_num = f"ORD{n}"
                    cur.execute(
                        "SELECT COUNT(*) AS cnt FROM orders WHERE order_num = %s",
                        (order_num,)
                    )
                    row = cur.fetchone()
                    cnt = row['cnt'] if isinstance(row, dict) else row[0]
                    if cnt == 0:
                        return order_num
            return f"ORD{random.randint(10000, 99999)}"
        except Exception as exc:
            logger.error(f"❌ Error generating order number: {exc}")
            import random as r
            return f"ORD{r.randint(10000, 99999)}"
        finally:
            if conn:
                self.put_connection(conn)

    def save_order(self, order_num: str, user_id: int, username: str, order_details: str) -> bool:
        conn = None
        try:
            conn = self.get_connection()
            order_json = json.dumps({"details": order_details})
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO order_confirmations
                            (order_num, user_id, username, order_details)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (order_num) DO UPDATE SET
                            user_id = EXCLUDED.user_id,
                            username = EXCLUDED.username,
                            order_details = EXCLUDED.order_details
                    """, (order_num, user_id, username, order_details))
                    cur.execute("""
                        INSERT INTO orders
                            (order_num, user_id, username, status, order_details)
                        VALUES (%s, %s, %s, 'pending', %s::jsonb)
                        ON CONFLICT (order_num) DO UPDATE SET
                            user_id = EXCLUDED.user_id,
                            username = EXCLUDED.username,
                            order_details = EXCLUDED.order_details
                    """, (order_num, user_id, username, order_json))
            logger.info(f"✅ Order {order_num} saved for user {user_id}")
            return True
        except Exception as exc:
            logger.error(f"❌ Error saving order: {exc}")
            return False
        finally:
            if conn:
                self.put_connection(conn)

    def get_pending_orders(self) -> List[Dict]:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT order_num, user_id, username, order_details, created_at
                    FROM order_confirmations
                    WHERE confirmed_by_admin = FALSE
                    ORDER BY created_at DESC
                """)
                return [dict(r) for r in cur.fetchall()]
        except Exception as exc:
            logger.error(f"❌ Error getting pending orders: {exc}")
            return []
        finally:
            if conn:
                self.put_connection(conn)

    def get_confirmed_orders(self, limit: int = 50) -> List[Dict]:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT order_num, user_id, username, order_details,
                           created_at, confirmation_date
                    FROM order_confirmations
                    WHERE confirmed_by_admin = TRUE
                    ORDER BY confirmation_date DESC
                    LIMIT %s
                """, (limit,))
                return [dict(r) for r in cur.fetchall()]
        except Exception as exc:
            logger.error(f"❌ Error getting confirmed orders: {exc}")
            return []
        finally:
            if conn:
                self.put_connection(conn)

    def cleanup_old_confirmed_orders(self, keep_last: int = 30) -> int:
        conn = None
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        DELETE FROM order_confirmations
                        WHERE id NOT IN (
                            SELECT id FROM order_confirmations
                            WHERE confirmed_by_admin = TRUE
                            ORDER BY confirmation_date DESC
                            LIMIT %s
                        )
                        AND confirmed_by_admin = TRUE
                    """, (keep_last,))
                    deleted = cur.rowcount
            logger.info(f"✅ Cleaned up {deleted} old confirmed orders")
            return deleted
        except Exception as exc:
            logger.error(f"❌ Error cleaning up orders: {exc}")
            return 0
        finally:
            if conn:
                self.put_connection(conn)

    def confirm_order_by_admin(self, order_num: str) -> Optional[Dict]:
        conn = None
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE order_confirmations
                        SET confirmed_by_admin = TRUE,
                            confirmation_date = CURRENT_TIMESTAMP
                        WHERE order_num = %s AND confirmed_by_admin = FALSE
                        RETURNING user_id, username, order_details
                    """, (order_num,))
                    result = cur.fetchone()
                    if result:
                        cur.execute("""
                            UPDATE orders
                            SET status = 'confirmed',
                                confirmed_at = CURRENT_TIMESTAMP
                            WHERE order_num = %s
                        """, (order_num,))
                        logger.info(f"✅ Order {order_num} confirmed by admin")
                        return dict(result)
                    logger.warning(f"⚠️ Order {order_num} not found or already confirmed")
                    return None
        except Exception as exc:
            logger.error(f"❌ Error confirming order: {exc}")
            return None
        finally:
            if conn:
                self.put_connection(conn)

    def _parse_order_dict(self, order_dict: Dict) -> Dict:
        """Add payment_method and total_price fields parsed from order_details JSON."""
        import re
        details_json = order_dict.get('order_details') or {}
        if isinstance(details_json, str):
            try:
                details_json = json.loads(details_json)
            except Exception:
                details_json = {}
        details_text = details_json.get('details', '') if isinstance(details_json, dict) else ''
        m = re.search(r'Total:\s*£([\d.]+)', details_text)
        order_dict['total_price'] = float(m.group(1)) if m else 0.0
        if 'PayPal' in details_text:
            order_dict['payment_method'] = 'PayPal'
        elif 'Litecoin' in details_text or 'LTC' in details_text:
            order_dict['payment_method'] = 'Litecoin'
        else:
            order_dict['payment_method'] = 'Unknown'
        return order_dict

    def get_user_orders(self, user_id: int, limit: int = 10) -> List[Dict]:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT order_num, user_id, username, status,
                           order_details, created_at, confirmed_at
                    FROM orders
                    WHERE user_id = %s
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (user_id, limit))
                return [self._parse_order_dict(dict(r)) for r in cur.fetchall()]
        except Exception as exc:
            logger.error(f"❌ Error getting user orders: {exc}")
            return []
        finally:
            if conn:
                self.put_connection(conn)

    def get_order_by_number(self, order_num) -> Optional[Dict]:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                order_str = str(order_num)
                if order_str.isdigit():
                    prefixed = f"ORD{order_str}"
                    cur.execute("""
                        SELECT order_num, user_id, username, status,
                               order_details, created_at, confirmed_at
                        FROM orders WHERE order_num = %s OR order_num = %s
                    """, (prefixed, order_str))
                else:
                    cur.execute("""
                        SELECT order_num, user_id, username, status,
                               order_details, created_at, confirmed_at
                        FROM orders WHERE order_num = %s
                    """, (order_str,))
                row = cur.fetchone()
                return self._parse_order_dict(dict(row)) if row else None
        except Exception as exc:
            logger.error(f"❌ Error getting order by number: {exc}")
            return None
        finally:
            if conn:
                self.put_connection(conn)

    def get_all_orders(self, limit: int = 1000) -> List[Dict]:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT order_num, user_id, username, status,
                           order_details, created_at, confirmed_at
                    FROM orders
                    ORDER BY created_at DESC
                    LIMIT %s
                """, (limit,))
                return [self._parse_order_dict(dict(r)) for r in cur.fetchall()]
        except Exception as exc:
            logger.error(f"❌ Error getting all orders: {exc}")
            return []
        finally:
            if conn:
                self.put_connection(conn)

    def delete_order(self, order_num) -> bool:
        conn = None
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM orders WHERE order_num = %s", (order_num,))
                    cur.execute("DELETE FROM order_confirmations WHERE order_num = %s", (order_num,))
            logger.info(f"✅ Order {order_num} deleted")
            return True
        except Exception as exc:
            logger.error(f"❌ Error deleting order: {exc}")
            return False
        finally:
            if conn:
                self.put_connection(conn)

    def get_sales_analytics(self, days: int = 30) -> Dict:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT order_details, created_at, confirmed_at
                    FROM orders
                    WHERE status = 'confirmed'
                      AND created_at >= NOW() - make_interval(days => %s)
                    ORDER BY created_at DESC
                """, (days,))
                orders = cur.fetchall()
            import re
            total_revenue = 0
            product_sales: Dict[str, int] = {}
            payment_methods: Dict[str, int] = {}
            for order in orders:
                od = order['order_details']
                if isinstance(od, str):
                    try:
                        od = json.loads(od)
                    except Exception:
                        continue
                total = 0
                if isinstance(od, dict):
                    if 'total' in od:
                        total = od['total']
                    elif 'details' in od:
                        m = re.search(r'Final Total.*?£([\d,]+\.?\d*)', od['details'])
                        if m:
                            total = float(m.group(1).replace(',', ''))
                total_revenue += total
                items = od.get('items', []) if isinstance(od, dict) else []
                for item in items:
                    n = item.get('name', 'Unknown')
                    product_sales[n] = product_sales.get(n, 0) + 1
                pm = 'Unknown'
                if isinstance(od, dict):
                    if 'payment_method' in od:
                        pm = od['payment_method']
                    elif 'details' in od:
                        m2 = re.search(r'Payment:\*\s+(.+?)\n', od['details'])
                        if m2:
                            pm = m2.group(1).strip()
                payment_methods[pm] = payment_methods.get(pm, 0) + 1
            top_products = sorted(product_sales.items(), key=lambda x: x[1], reverse=True)[:5]
            return {
                'total_revenue': total_revenue,
                'total_orders': len(orders),
                'top_products': top_products,
                'payment_methods': payment_methods,
            }
        except Exception as exc:
            logger.error(f"❌ Error getting analytics: {exc}")
            return {'total_revenue': 0, 'total_orders': 0, 'top_products': [], 'payment_methods': {}}
        finally:
            if conn:
                self.put_connection(conn)

    # ------------------------------------------------------------------ #
    #  Blocked users                                                       #
    # ------------------------------------------------------------------ #

    def block_user(self, user_id: int, username=None, blocked_by: str = "admin", reason=None) -> bool:
        conn = None
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO blocked_users (user_id, username, blocked_by, reason)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (user_id) DO UPDATE SET
                            username = EXCLUDED.username,
                            blocked_by = EXCLUDED.blocked_by,
                            reason = EXCLUDED.reason,
                            blocked_at = CURRENT_TIMESTAMP
                    """, (user_id, username, blocked_by, reason))
            logger.info(f"✅ User {user_id} blocked")
            return True
        except Exception as exc:
            logger.error(f"❌ Error blocking user: {exc}")
            return False
        finally:
            if conn:
                self.put_connection(conn)

    def unblock_user(self, user_id: int) -> bool:
        conn = None
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM blocked_users WHERE user_id = %s", (user_id,))
            logger.info(f"✅ User {user_id} unblocked")
            return True
        except Exception as exc:
            logger.error(f"❌ Error unblocking user: {exc}")
            return False
        finally:
            if conn:
                self.put_connection(conn)

    def is_user_blocked(self, user_id: int) -> bool:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM blocked_users WHERE user_id = %s", (user_id,))
                return cur.fetchone() is not None
        except Exception as exc:
            logger.error(f"❌ Error checking blocked status: {exc}")
            return False
        finally:
            if conn:
                self.put_connection(conn)

    def get_blocked_users(self) -> List[Dict]:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT user_id, username, blocked_by, reason, blocked_at
                    FROM blocked_users ORDER BY blocked_at DESC
                """)
                return [dict(r) for r in cur.fetchall()]
        except Exception as exc:
            logger.error(f"❌ Error getting blocked users: {exc}")
            return []
        finally:
            if conn:
                self.put_connection(conn)

    # ------------------------------------------------------------------ #
    #  Broadcast users                                                     #
    # ------------------------------------------------------------------ #

    def add_broadcast_user(self, user_id: int, username=None,
                           first_name=None, last_name=None) -> bool:
        conn = None
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO broadcast_users
                            (user_id, username, first_name, last_name, last_seen)
                        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (user_id) DO UPDATE SET
                            username = COALESCE(%s, broadcast_users.username),
                            first_name = COALESCE(%s, broadcast_users.first_name),
                            last_name = COALESCE(%s, broadcast_users.last_name),
                            last_seen = CURRENT_TIMESTAMP,
                            active = TRUE
                    """, (user_id, username, first_name, last_name,
                          username, first_name, last_name))
            return True
        except Exception as exc:
            logger.error(f"❌ Error adding broadcast user {user_id}: {exc}")
            return False
        finally:
            if conn:
                self.put_connection(conn)

    def get_broadcast_users(self) -> List[int]:
        for attempt in range(3):
            conn = None
            try:
                conn = self.get_connection()
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT user_id FROM broadcast_users
                        WHERE active = TRUE
                        ORDER BY last_seen DESC
                    """)
                    rows = cur.fetchall()
                users = [int(r['user_id']) for r in rows]
                logger.info(f"✅ Retrieved {len(users)} broadcast users (attempt {attempt + 1})")
                return users
            except Exception as exc:
                logger.error(f"❌ Error getting broadcast users (attempt {attempt + 1}/3): {exc}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
            finally:
                if conn:
                    self.put_connection(conn)
        logger.critical("❌ CRITICAL: Failed to get broadcast users after all retry attempts")
        return []

    def get_all_broadcast_users(self) -> List[Dict]:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT user_id, username, first_name, last_name,
                           last_seen, first_interaction
                    FROM broadcast_users
                    WHERE active = TRUE
                    ORDER BY last_seen DESC
                """)
                return [dict(r) for r in cur.fetchall()]
        except Exception as exc:
            logger.error(f"❌ Error getting all broadcast users: {exc}")
            return []
        finally:
            if conn:
                self.put_connection(conn)

    def get_user_info(self, user_id: int) -> Optional[Dict]:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT user_id, username, first_name, last_name,
                           last_seen, first_interaction
                    FROM broadcast_users WHERE user_id = %s
                """, (user_id,))
                row = cur.fetchone()
                return dict(row) if row else None
        except Exception as exc:
            logger.error(f"❌ Error getting user info: {exc}")
            return None
        finally:
            if conn:
                self.put_connection(conn)

    def get_broadcast_user_count(self) -> int:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS cnt FROM broadcast_users WHERE active = TRUE")
                row = cur.fetchone()
                return row['cnt'] if isinstance(row, dict) else row[0]
        except Exception as exc:
            logger.error(f"❌ Error getting broadcast user count: {exc}")
            return 0
        finally:
            if conn:
                self.put_connection(conn)

    def deactivate_broadcast_user(self, user_id: int) -> bool:
        conn = None
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE broadcast_users SET active = FALSE WHERE user_id = %s",
                        (user_id,)
                    )
                    return cur.rowcount > 0
        except Exception as exc:
            logger.error(f"❌ Error deactivating user: {exc}")
            return False
        finally:
            if conn:
                self.put_connection(conn)

    def restore_all_users_from_history(self) -> int:
        conn = None
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO broadcast_users
                            (user_id, first_interaction, last_seen, active)
                        SELECT DISTINCT user_id,
                               CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE
                        FROM (
                            SELECT user_id FROM order_confirmations
                            UNION
                            SELECT user_id FROM reviews
                            UNION
                            SELECT user_id FROM broadcast_users
                        ) AS all_users
                        WHERE user_id IS NOT NULL AND user_id != 12345
                        ON CONFLICT (user_id) DO UPDATE SET
                            active = TRUE,
                            last_seen = CURRENT_TIMESTAMP
                    """)
                    count = cur.rowcount
            logger.info(f"✅ Bulletproof restore: {count} users added/reactivated")
            return count
        except Exception as exc:
            logger.error(f"❌ Error in bulletproof restore: {exc}")
            return 0
        finally:
            if conn:
                self.put_connection(conn)

    # ------------------------------------------------------------------ #
    #  Discounts / Coupons / Promotions                                    #
    # ------------------------------------------------------------------ #

    def create_promotion(self, promo_type: str, name: str, description: str = "",
                         target_products=None, percent_off: float = 0,
                         amount_off: float = 0, buy_qty: int = 1, get_qty: int = 0,
                         min_spend: float = 0, end_at=None, stackable: bool = False) -> bool:
        conn = None
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO promotions
                            (type, name, description, target_product_ids,
                             percent_off, amount_off, buy_qty, get_qty,
                             min_spend, end_at, stackable)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (promo_type, name, description,
                          json.dumps(target_products or []),
                          percent_off, amount_off, buy_qty, get_qty,
                          min_spend, end_at, stackable))
            logger.info(f"✅ Created promotion: {name}")
            return True
        except Exception as exc:
            logger.error(f"❌ Error creating promotion: {exc}")
            return False
        finally:
            if conn:
                self.put_connection(conn)

    def get_active_promotions(self) -> List[Dict]:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM promotions
                    WHERE active = TRUE
                      AND (end_at IS NULL OR end_at > CURRENT_TIMESTAMP)
                    ORDER BY created_at DESC
                """)
                result = []
                for row in cur.fetchall():
                    p = dict(row)
                    for field in ('target_product_ids', 'segments'):
                        v = p.get(field)
                        if isinstance(v, str):
                            p[field] = json.loads(v or '[]')
                        elif v is None:
                            p[field] = []
                    result.append(p)
                return result
        except Exception as exc:
            logger.error(f"❌ Error getting active promotions: {exc}")
            return []
        finally:
            if conn:
                self.put_connection(conn)

    def get_all_promotions(self) -> List[Dict]:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM promotions ORDER BY created_at DESC")
                result = []
                for row in cur.fetchall():
                    p = dict(row)
                    for field in ('target_product_ids', 'segments'):
                        v = p.get(field)
                        if isinstance(v, str):
                            p[field] = json.loads(v or '[]')
                        elif v is None:
                            p[field] = []
                    result.append(p)
                return result
        except Exception as exc:
            logger.error(f"❌ Error getting all promotions: {exc}")
            return []
        finally:
            if conn:
                self.put_connection(conn)

    def toggle_promotion(self, promo_id: int) -> bool:
        conn = None
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE promotions SET active = NOT active WHERE id = %s",
                        (promo_id,)
                    )
            return True
        except Exception as exc:
            logger.error(f"❌ Error toggling promotion: {exc}")
            return False
        finally:
            if conn:
                self.put_connection(conn)

    def update_promotion_status(self, promo_id: int, active: bool) -> bool:
        conn = None
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE promotions SET active = %s WHERE id = %s",
                        (active, promo_id)
                    )
            return True
        except Exception as exc:
            logger.error(f"❌ Error updating promotion status: {exc}")
            return False
        finally:
            if conn:
                self.put_connection(conn)

    def create_coupon(self, code: str, coupon_type: str, value: float,
                      min_spend: float = 0, expires_at=None,
                      max_uses: int = 1000, per_user_limit: int = 1) -> Dict:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT code FROM coupons WHERE UPPER(code) = UPPER(%s)", (code,)
                )
                if cur.fetchone():
                    return {"success": False, "error": f"Coupon code '{code}' already exists"}
                cur.execute("""
                    INSERT INTO coupons
                        (code, type, value, min_spend, expires_at, max_uses, per_user_limit)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (code.upper(), coupon_type, value, min_spend,
                      expires_at, max_uses, per_user_limit))
                conn.commit()
            logger.info(f"✅ Created coupon: {code}")
            return {"success": True, "message": f"Coupon '{code}' created"}
        except Exception as exc:
            logger.error(f"❌ Error creating coupon: {exc}")
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            return {"success": False, "error": "Database error"}
        finally:
            if conn:
                self.put_connection(conn)

    def validate_coupon(self, code: str, user_id) -> Optional[Dict]:
        """Validate coupon and return coupon dict if valid, else None."""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM coupons
                    WHERE UPPER(code) = UPPER(%s)
                      AND active = TRUE
                      AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
                      AND current_uses < max_uses
                """, (code,))
                row = cur.fetchone()
                if not row:
                    return None
                coupon = dict(row)
                if coupon.get('per_user_limit', 1) > 0:
                    cur.execute("""
                        SELECT COUNT(*) AS cnt FROM coupon_usage
                        WHERE coupon_code = %s AND user_id = %s
                    """, (coupon['code'], str(user_id)))
                    usage_row = cur.fetchone()
                    cnt = usage_row['cnt'] if isinstance(usage_row, dict) else usage_row[0]
                    if cnt >= coupon['per_user_limit']:
                        return None
                return coupon
        except Exception as exc:
            logger.error(f"❌ Error validating coupon: {exc}")
            return None
        finally:
            if conn:
                self.put_connection(conn)

    def use_coupon(self, code: str, user_id: int, order_num: str) -> bool:
        conn = None
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO coupon_usage (coupon_code, user_id, order_num)
                        VALUES (%s, %s, %s)
                    """, (code.upper(), user_id, order_num))
                    cur.execute("""
                        UPDATE coupons SET current_uses = current_uses + 1
                        WHERE code = %s
                    """, (code.upper(),))
            logger.info(f"✅ Coupon {code} used by {user_id}")
            return True
        except Exception as exc:
            logger.error(f"❌ Error using coupon: {exc}")
            return False
        finally:
            if conn:
                self.put_connection(conn)

    # ------------------------------------------------------------------ #
    #  Bot instance locking                                                #
    # ------------------------------------------------------------------ #

    def acquire_bot_instance_lock(self, instance_id: str,
                                  lease_duration_minutes: int = 5) -> bool:
        import socket
        host_info = f"PID:{os.getpid()}, Host:{socket.gethostname()}"
        conn = None
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM bot_instance WHERE lease_until < NOW()")
                    try:
                        cur.execute("""
                            INSERT INTO bot_instance
                                (instance_id, lease_until, heartbeat_at, host_info)
                            VALUES (%s,
                                    NOW() + INTERVAL '1 minute' * %s,
                                    NOW(), %s)
                        """, (instance_id, lease_duration_minutes, host_info))
                        logger.info(f"✅ Lock acquired: {instance_id}")
                        return True
                    except Exception as insert_err:
                        if "duplicate key" in str(insert_err) or "already exists" in str(insert_err):
                            cur.execute("""
                                UPDATE bot_instance
                                SET lease_until = NOW() + INTERVAL '1 minute' * %s,
                                    heartbeat_at = NOW(),
                                    host_info = %s
                                WHERE instance_id = %s AND lease_until < NOW()
                                RETURNING id
                            """, (lease_duration_minutes, host_info, instance_id))
                            if cur.fetchone():
                                logger.info(f"✅ Lock acquired (renewed): {instance_id}")
                                return True
                            logger.warning("⚠️ Lock held by another instance")
                            return False
                        raise
        except Exception as exc:
            logger.error(f"❌ Error acquiring lock: {exc}")
            return False
        finally:
            if conn:
                self.put_connection(conn)

    def renew_bot_instance_lock(self, instance_id: str,
                                lease_duration_minutes: int = 5) -> bool:
        conn = None
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE bot_instance
                        SET lease_until = CURRENT_TIMESTAMP + %s * INTERVAL '1 minute',
                            heartbeat_at = CURRENT_TIMESTAMP
                        WHERE instance_id = %s AND lease_until > CURRENT_TIMESTAMP
                        RETURNING id
                    """, (lease_duration_minutes, instance_id))
                    return cur.fetchone() is not None
        except Exception as exc:
            logger.error(f"❌ Error renewing lock: {exc}")
            return False
        finally:
            if conn:
                self.put_connection(conn)

    def release_bot_instance_lock(self, instance_id: str) -> bool:
        conn = None
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM bot_instance WHERE instance_id = %s",
                        (instance_id,)
                    )
                    return cur.rowcount > 0
        except Exception as exc:
            logger.error(f"❌ Error releasing lock: {exc}")
            return False
        finally:
            if conn:
                self.put_connection(conn)

    def check_bot_instance_lock(self, instance_id: str) -> bool:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT lease_until > CURRENT_TIMESTAMP AS valid
                    FROM bot_instance WHERE instance_id = %s
                """, (instance_id,))
                row = cur.fetchone()
                return bool(row['valid'] if isinstance(row, dict) else row[0]) if row else False
        except Exception as exc:
            logger.error(f"❌ Error checking lock: {exc}")
            return False
        finally:
            if conn:
                self.put_connection(conn)

    def acquire_single_instance_lock(self, instance_id: str,
                                     timeout_minutes: int = 5) -> bool:
        from datetime import datetime, timedelta
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS bot_instance_lock (
                        lock_name VARCHAR(50) PRIMARY KEY,
                        instance_id VARCHAR(100) NOT NULL,
                        acquired_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        expires_at TIMESTAMP NOT NULL
                    )
                """)
                expires_at = datetime.now() + timedelta(minutes=timeout_minutes)
                cur.execute("""
                    INSERT INTO bot_instance_lock (lock_name, instance_id, expires_at)
                    VALUES ('main_bot', %s, %s)
                    ON CONFLICT (lock_name) DO UPDATE SET
                        instance_id = %s,
                        acquired_at = CURRENT_TIMESTAMP,
                        expires_at = %s
                    WHERE bot_instance_lock.expires_at < CURRENT_TIMESTAMP
                """, (instance_id, expires_at, instance_id, expires_at))
                cur.execute(
                    "SELECT instance_id FROM bot_instance_lock WHERE lock_name = 'main_bot'"
                )
                row = cur.fetchone()
                conn.commit()
                held = (row['instance_id'] if isinstance(row, dict) else row[0]) == instance_id
                if held:
                    logger.info(f"✅ Single-instance lock acquired: {instance_id}")
                else:
                    logger.warning("❌ Lock held by another instance")
                return held
        except Exception as exc:
            logger.error(f"❌ Error acquiring single-instance lock: {exc}")
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            return False
        finally:
            if conn:
                self.put_connection(conn)

    def release_single_instance_lock(self, instance_id: str) -> bool:
        conn = None
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        DELETE FROM bot_instance_lock
                        WHERE lock_name = 'main_bot' AND instance_id = %s
                    """, (instance_id,))
                    return cur.rowcount > 0
        except Exception as exc:
            logger.error(f"❌ Error releasing single-instance lock: {exc}")
            return False
        finally:
            if conn:
                self.put_connection(conn)

    # ------------------------------------------------------------------ #
    #  Menu management                                                     #
    # ------------------------------------------------------------------ #

    def add_category(self, name: str, display_order: int = 0) -> Dict:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO menu_categories (name, display_order)
                    VALUES (%s, %s) RETURNING id
                """, (name, display_order))
                category_id = cur.fetchone()['id']
                conn.commit()
            return {"success": True, "id": category_id, "message": f"Category '{name}' created"}
        except Exception as exc:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.error(f"❌ Error adding category: {exc}")
            return {"success": False, "error": str(exc)}
        finally:
            if conn:
                self.put_connection(conn)

    def get_categories(self) -> List[Dict]:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, name, display_order
                    FROM menu_categories
                    WHERE active = TRUE
                    ORDER BY display_order, name
                """)
                return [dict(r) for r in cur.fetchall()]
        except Exception as exc:
            logger.error(f"❌ Error getting categories: {exc}")
            return []
        finally:
            if conn:
                self.put_connection(conn)

    def delete_category(self, category_name: str) -> Dict:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id FROM menu_categories
                    WHERE name = %s AND active = TRUE
                """, (category_name,))
                row = cur.fetchone()
                if not row:
                    return {"success": False, "error": f"Category '{category_name}' not found"}
                cat_id = row['id']
                cur.execute(
                    "UPDATE menu_products SET active = FALSE WHERE category_id = %s",
                    (cat_id,)
                )
                cur.execute(
                    "UPDATE menu_categories SET active = FALSE WHERE id = %s",
                    (cat_id,)
                )
                conn.commit()
            return {"success": True, "message": f"Category '{category_name}' deleted"}
        except Exception as exc:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.error(f"❌ Error deleting category: {exc}")
            return {"success": False, "error": str(exc)}
        finally:
            if conn:
                self.put_connection(conn)

    def add_product(self, category_id: int, name: str, description=None) -> Dict:
        """Add a product, or return/update the existing product safely.

        Important: this does NOT force an existing inactive product back on.
        That keeps admin toggles stable across bot restarts and migrations.
        """
        conn = None
        try:
            clean_name = str(name).strip()[:200]
            if not clean_name:
                return {"success": False, "error": "Product name is required"}
            clean_description = str(description).strip() if description else None
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO menu_products (category_id, name, description)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (category_id, name) DO UPDATE SET
                        description = COALESCE(EXCLUDED.description, menu_products.description)
                    RETURNING id
                """, (category_id, clean_name, clean_description))
                product_id = cur.fetchone()['id']
                conn.commit()
            return {"success": True, "id": product_id, "message": f"Product '{clean_name}' saved"}
        except Exception as exc:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.error(f"❌ Error adding product: {exc}")
            return {"success": False, "error": str(exc)}
        finally:
            if conn:
                self.put_connection(conn)

    def add_pricing_tier(self, product_id: int, size: str, price: float,
                         display_order: int = 0) -> Dict:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO menu_pricing (product_id, size, price, display_order)
                    VALUES (%s, %s, %s, %s) RETURNING id
                """, (product_id, size, price, display_order))
                tier_id = cur.fetchone()['id']
                conn.commit()
            return {"success": True, "id": tier_id}
        except Exception as exc:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.error(f"❌ Error adding pricing tier: {exc}")
            return {"success": False, "error": str(exc)}
        finally:
            if conn:
                self.put_connection(conn)

    def update_product_pricing(self, category: str, product_name: str,
                                pricing_tiers: List[Tuple[str, float]]) -> Dict:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT p.id FROM menu_products p
                    JOIN menu_categories c ON p.category_id = c.id
                    WHERE c.name = %s AND p.name = %s
                """, (category, product_name))
                row = cur.fetchone()
                if not row:
                    return {'success': False, 'error': f'Product "{product_name}" not found'}
                product_id = row['id']
                cur.execute("DELETE FROM menu_pricing WHERE product_id = %s", (product_id,))
                for order, (size, price) in enumerate(pricing_tiers):
                    cur.execute("""
                        INSERT INTO menu_pricing (product_id, size, price, display_order)
                        VALUES (%s, %s, %s, %s)
                    """, (product_id, size, float(price), order))
                # Do not force active=True here. Admin toggle state must survive edits/migrations.
                conn.commit()
            return {'success': True}
        except Exception as exc:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.error(f"❌ Error updating pricing: {exc}")
            return {'success': False, 'error': str(exc)}
        finally:
            if conn:
                self.put_connection(conn)

    def rename_product(self, category: str, old_name: str, new_name: str) -> Dict:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT p.id FROM menu_products p
                    JOIN menu_categories c ON p.category_id = c.id
                    WHERE c.name = %s AND p.name = %s
                """, (category, old_name))
                row = cur.fetchone()
                if not row:
                    return {'success': False, 'error': f'Product "{old_name}" not found'}
                cur.execute(
                    "UPDATE menu_products SET name = %s WHERE id = %s",
                    (new_name, row['id'])
                )
                conn.commit()
            return {'success': True}
        except Exception as exc:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.error(f"❌ Error renaming product: {exc}")
            return {'success': False, 'error': str(exc)}
        finally:
            if conn:
                self.put_connection(conn)

    def get_dynamic_menu(self) -> Dict:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT c.name AS category_name,
                           p.name AS product_name,
                           p.description AS product_description,
                           pr.size,
                           pr.price
                    FROM menu_categories c
                    LEFT JOIN menu_products p
                        ON c.id = p.category_id AND p.active = TRUE
                    LEFT JOIN menu_pricing pr
                        ON p.id = pr.product_id AND pr.active = TRUE
                    WHERE c.active = TRUE
                    ORDER BY c.display_order, c.name, p.name,
                             pr.display_order, pr.size
                """)
                rows = cur.fetchall()
            menu: Dict = {}
            descriptions: Dict = {}
            for row in rows:
                cat = row['category_name']
                prod = row['product_name']
                desc = row['product_description']
                size = row['size']
                price = row['price']
                if cat not in menu:
                    menu[cat] = {}
                if prod and desc:
                    descriptions[prod] = desc
                if prod and size and price is not None:
                    if prod not in menu[cat]:
                        menu[cat][prod] = []
                    menu[cat][prod].append((size, int(price)))
            return {'menu': menu, 'descriptions': descriptions}
        except Exception as exc:
            logger.error(f"❌ Error getting dynamic menu: {exc}")
            return {'menu': {}, 'descriptions': {}}
        finally:
            if conn:
                self.put_connection(conn)

    def remove_product(self, category_name: str, product_name: str) -> Dict:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE menu_products SET active = FALSE
                    WHERE name = %s AND category_id = (
                        SELECT id FROM menu_categories WHERE name = %s
                    )
                """, (product_name, category_name))
                if cur.rowcount > 0:
                    conn.commit()
                    return {"success": True, "message": f"Product '{product_name}' removed"}
                return {"success": False, "error": "Product not found"}
        except Exception as exc:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.error(f"❌ Error removing product: {exc}")
            return {"success": False, "error": str(exc)}
        finally:
            if conn:
                self.put_connection(conn)

    def toggle_product_status(self, category_name: str, product_name: str) -> Dict:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT p.active FROM menu_products p
                    JOIN menu_categories c ON p.category_id = c.id
                    WHERE c.name = %s AND p.name = %s
                """, (category_name, product_name))
                row = cur.fetchone()
                if not row:
                    return {"success": False, "error": "Product not found"}
                new_status = not (row['active'] if isinstance(row, dict) else row[0])
                cur.execute("""
                    UPDATE menu_products SET active = %s
                    WHERE name = %s AND category_id = (
                        SELECT id FROM menu_categories WHERE name = %s
                    )
                """, (new_status, product_name, category_name))
                conn.commit()
            return {
                "success": True,
                "message": f"Product '{product_name}' {'activated' if new_status else 'deactivated'}",
                "new_status": "active" if new_status else "inactive",
            }
        except Exception as exc:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.error(f"❌ Error toggling product: {exc}")
            return {"success": False, "error": str(exc)}
        finally:
            if conn:
                self.put_connection(conn)

    def get_all_products_with_status(self) -> List[Dict]:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT c.name AS category, p.name AS product,
                           p.active, p.description
                    FROM menu_products p
                    JOIN menu_categories c ON p.category_id = c.id
                    WHERE c.active = TRUE
                    ORDER BY c.display_order, c.name, p.name
                """)
                return [dict(r) for r in cur.fetchall()]
        except Exception as exc:
            logger.error(f"❌ Error getting all products: {exc}")
            return []
        finally:
            if conn:
                self.put_connection(conn)

    def get_full_menu_with_prices(self) -> List[Dict]:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT c.name AS category, p.name AS product,
                           p.active, p.description, pr.size, pr.price
                    FROM menu_categories c
                    LEFT JOIN menu_products p ON c.id = p.category_id
                    LEFT JOIN menu_pricing pr
                        ON p.id = pr.product_id AND pr.active = TRUE
                    WHERE c.active = TRUE
                    ORDER BY c.display_order, c.name,
                             COALESCE((SELECT MIN(price) FROM menu_pricing
                                       WHERE product_id = p.id AND active = TRUE), 999999),
                             p.name, pr.price
                """)
                return [
                    {
                        'category': r['category'],
                        'product': r['product'],
                        'active': r['active'],
                        'description': r['description'] or '',
                        'size': r['size'] or 'N/A',
                        'price': float(r['price']) if r['price'] else 0,
                    }
                    for r in cur.fetchall() if r['product']
                ]
        except Exception as exc:
            logger.error(f"❌ Error getting full menu: {exc}")
            return []
        finally:
            if conn:
                self.put_connection(conn)

    def update_product_descriptions(self, descriptions_dict: Dict):
        conn = None
        try:
            conn = self.get_connection()
            updated = 0
            with conn.cursor() as cur:
                for name, desc in descriptions_dict.items():
                    cur.execute("""
                        UPDATE menu_products SET description = %s
                        WHERE name = %s AND active = TRUE
                    """, (desc, name))
                    updated += cur.rowcount
                conn.commit()
            if updated:
                logger.info(f"✅ Updated descriptions for {updated} products")
        except Exception as exc:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.error(f"❌ Error updating descriptions: {exc}")
        finally:
            if conn:
                self.put_connection(conn)

    def update_product_description(self, category_name: str, product_name: str,
                                   description: str) -> Dict:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE menu_products SET description = %s
                    WHERE name = %s AND category_id = (
                        SELECT id FROM menu_categories
                        WHERE name = %s AND active = TRUE
                    ) AND active = TRUE
                """, (description, product_name, category_name))
                if cur.rowcount > 0:
                    conn.commit()
                    return {"success": True, "message": "Description updated"}
                return {"success": False, "error": "Product not found"}
        except Exception as exc:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.error(f"❌ Error updating description: {exc}")
            return {"success": False, "error": str(exc)}
        finally:
            if conn:
                self.put_connection(conn)

    def migrate_existing_products(self, products_dict: Dict):
        """Idempotently sync the bundled fallback menu into the database.

        This is safe to run on every restart: it will not duplicate pricing tiers
        and it will not re-enable products that an admin has toggled off.
        """
        logger.info("🔄 Syncing bundled products to database...")
        for order, (cat_name, cat_products) in enumerate(products_dict.items()):
            result = self.add_category(cat_name, order)
            if not result.get('success'):
                logger.warning(f"Could not sync category {cat_name}: {result.get('error')}")
                continue
            cat_id = result['id']
            for prod_name, tiers in cat_products.items():
                prod_result = self.add_product(cat_id, prod_name)
                if not prod_result.get('success'):
                    logger.warning(f"Could not sync product {prod_name}: {prod_result.get('error')}")
                    continue
                # Replace pricing tiers instead of appending duplicates on each restart.
                self.update_product_pricing(cat_name, prod_name, [(size, float(price)) for size, price in tiers])
        logger.info("✅ Product sync completed")

    def set_products_active_by_name(self, product_names: List[str], active: bool = False) -> int:
        """Bulk enable/disable products by exact name. Returns affected row count."""
        if not product_names:
            return 0
        conn = None
        try:
            clean_names = [str(name).strip() for name in product_names if str(name).strip()]
            if not clean_names:
                return 0
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE menu_products SET active = %s WHERE name = ANY(%s)",
                    (active, clean_names)
                )
                affected = cur.rowcount
                conn.commit()
            logger.info(f"✅ Set {affected} products active={active}")
            return affected
        except Exception as exc:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.error(f"❌ Error bulk toggling products: {exc}")
            return 0
        finally:
            if conn:
                self.put_connection(conn)

    # ------------------------------------------------------------------ #
    #  Freebies                                                            #
    # ------------------------------------------------------------------ #

    def has_claimed_freebie(self, user_id: int) -> bool:
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM freebie_claims WHERE user_id = %s", (user_id,)
                )
                return cur.fetchone() is not None
        except Exception as exc:
            logger.error(f"❌ Error checking freebie: {exc}")
            return False
        finally:
            if conn:
                self.put_connection(conn)

    def claim_freebie(self, user_id: int, username: str, product_name: str) -> Dict:
        conn = None
        try:
            if self.has_claimed_freebie(user_id):
                return {"success": False, "error": "Already claimed"}
            conn = self.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO freebie_claims (user_id, username, claimed_product)
                    VALUES (%s, %s, %s)
                """, (user_id, username, product_name))
                conn.commit()
            return {"success": True, "message": f"Freebie '{product_name}' claimed"}
        except Exception as exc:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.error(f"❌ Error claiming freebie: {exc}")
            return {"success": False, "error": str(exc)}
        finally:
            if conn:
                self.put_connection(conn)


# ------------------------------------------------------------------ #
#  Module-level singleton                                              #
# ------------------------------------------------------------------ #

db_manager: Optional[DatabaseManager] = None


def get_db_manager() -> DatabaseManager:
    global db_manager
    if db_manager is None:
        db_manager = DatabaseManager()
        db_manager.init_tables()
    return db_manager
