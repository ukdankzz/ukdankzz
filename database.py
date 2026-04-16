import os
import psycopg2
import psycopg2.extras
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
import asyncio
from typing import List, Dict, Optional, Any, Tuple
import logging
import json

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Handles all database operations for the UKDANKZZ bot with connection pooling for instant responses"""
    
    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL')
        if not self.database_url:
            raise ValueError("DATABASE_URL environment variable is required")
        
        # Initialize connection pool for FAST responses (reuse connections instead of creating new ones)
        try:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=2,  # Minimum connections
                maxconn=10,  # Maximum connections
                dsn=self.database_url,
                cursor_factory=RealDictCursor,
                connect_timeout=10,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5
            )
            logger.info("✅ Database connection pool initialized (2-10 connections)")
        except Exception as e:
            logger.error(f"❌ Failed to create connection pool: {e}")
            self.connection_pool = None
    
    def health_check(self) -> bool:
        """Check if database is accessible and healthy"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            logger.info("✅ Database health check passed")
            return True
        except Exception as e:
            logger.error(f"❌ Database health check failed: {e}")
            return False
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
            
    def get_connection(self, retries=3):
        """Get a connection from the pool (FAST - reuses existing connections)"""
        if self.connection_pool:
            try:
                # Get connection from pool - INSTANT (no new connection creation)
                conn = self.connection_pool.getconn()
                return conn
            except Exception as e:
                logger.error(f"❌ Failed to get connection from pool: {e}")
                # Fallback to direct connection if pool fails
        
        # Fallback: Create new connection (only if pool unavailable)
        last_exception = None
        for attempt in range(retries):
            try:
                conn = psycopg2.connect(
                    self.database_url,
                    cursor_factory=RealDictCursor,
                    connect_timeout=10,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5
                )
                return conn
            except Exception as e:
                last_exception = e
                if attempt < retries - 1:
                    import time
                    time.sleep(1)
                    continue
        
        logger.error(f"❌ All database connection attempts failed")
        raise last_exception or psycopg2.OperationalError("Failed to connect to database")
    
    def put_connection(self, conn):
        """Return connection to pool for reuse (critical for performance)"""
        if conn and self.connection_pool:
            try:
                self.connection_pool.putconn(conn)
            except Exception as e:
                logger.warning(f"⚠️ Failed to return connection to pool: {e}")
                try:
                    self.put_connection(conn)
                except:
                    pass
        elif conn:
            try:
                self.put_connection(conn)
            except:
                pass
    
    def init_tables(self):
        """Initialize all required database tables"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                    # Create reviews table
                    cursor.execute("""
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
                    
                    # Create orders table for tracking
                    cursor.execute("""
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
                    
                    # Create order confirmations table
                    cursor.execute("""
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
                    
                    # Create promotions table for discount system
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS promotions (
                            id SERIAL PRIMARY KEY,
                            type VARCHAR(20) NOT NULL CHECK (type IN ('bundle', 'item', 'collection', 'flash')),
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
                    
                    # Create coupons table for discount codes
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS coupons (
                            code VARCHAR(50) PRIMARY KEY,
                            type VARCHAR(10) NOT NULL CHECK (type IN ('percent', 'fixed')),
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
                    
                    # Create coupon usage tracking table
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS coupon_usage (
                            id SERIAL PRIMARY KEY,
                            coupon_code VARCHAR(50) REFERENCES coupons(code) ON DELETE CASCADE,
                            user_id BIGINT NOT NULL,
                            order_num VARCHAR(50),
                            used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    # Create bot instance table for distributed locking
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS bot_instance (
                            id SERIAL PRIMARY KEY,
                            instance_id VARCHAR(255) UNIQUE NOT NULL,
                            lease_until TIMESTAMP NOT NULL,
                            heartbeat_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            host_info VARCHAR(500),
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    # Create broadcast users table for bulletproof user persistence
                    cursor.execute("""
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
                    
                    # Create blocked users table for user management
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS blocked_users (
                            user_id BIGINT PRIMARY KEY,
                            username VARCHAR(255),
                            blocked_by VARCHAR(255),
                            reason TEXT,
                            blocked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    # Create dynamic menu management tables
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS menu_categories (
                            id SERIAL PRIMARY KEY,
                            name VARCHAR(100) NOT NULL UNIQUE,
                            display_order INTEGER DEFAULT 0,
                            active BOOLEAN DEFAULT TRUE,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    cursor.execute("""
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
                    
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS menu_pricing (
                            id SERIAL PRIMARY KEY,
                            product_id INTEGER REFERENCES menu_products(id) ON DELETE CASCADE,
                            size VARCHAR(50) NOT NULL,
                            price NUMERIC(10,2) NOT NULL,
                            display_order INTEGER DEFAULT 0,
                            active BOOLEAN DEFAULT TRUE
                        )
                    """)
                    
                    # Create first-time freebie tracking table
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS freebie_claims (
                            id SERIAL PRIMARY KEY,
                            user_id BIGINT NOT NULL UNIQUE,
                            username VARCHAR(255),
                            claimed_product VARCHAR(255),
                            claimed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    # Create indexes for faster queries
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_reviews_user_id ON reviews(user_id);
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_promotions_active ON promotions(active, start_at, end_at);
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_coupons_active ON coupons(active, expires_at);
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_coupon_usage_user ON coupon_usage(user_id, coupon_code);
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_reviews_order_num ON reviews(order_num);
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_reviews_date ON reviews(review_date);
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_orders_user_id ON order_confirmations(user_id);
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_orders_order_num ON order_confirmations(order_num);
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_bot_instance_lease ON bot_instance(instance_id, lease_until);
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_broadcast_users_active ON broadcast_users(active, last_seen);
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_blocked_users ON blocked_users(user_id);
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_menu_categories_order ON menu_categories(display_order, active);
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_menu_products_category ON menu_products(category_id, active);
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_menu_pricing_product ON menu_pricing(product_id, display_order);
                    """)
                    cursor.execute("""
                        CREATE INDEX IF NOT EXISTS idx_freebie_claims_user ON freebie_claims(user_id);
                    """)
                    
                    conn.commit()
                    logger.info("✅ Database tables initialized successfully")
                    
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            logger.error(f"❌ Error initializing database tables: {e}")
            raise
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def save_review(self, username: str, rating: int, review_text: str, 
                   review_date, order_items: List[Dict], order_num: str, 
                   user_id: int) -> bool:
        """Save a review to the database with comprehensive error handling and validation"""
        
        # Input validation
        try:
            # Validate and sanitize inputs
            if not username or len(str(username).strip()) == 0:
                logger.error("❌ Invalid username for review")
                return False
                
            username = str(username).strip()[:255]  # Limit length
            
            if not isinstance(rating, int) or rating < 1 or rating > 5:
                logger.error(f"❌ Invalid rating: {rating}")
                return False
                
            if not review_text or len(str(review_text).strip()) < 3:
                logger.error("❌ Review text too short")
                return False
                
            review_text = str(review_text).strip()[:2000]  # Limit length
            
            if not order_num or len(str(order_num).strip()) == 0:
                logger.error("❌ Invalid order number")
                return False
                
            order_num = str(order_num).strip()[:50]
            
            if not isinstance(user_id, int) or user_id <= 0:
                logger.error(f"❌ Invalid user_id: {user_id}")
                return False
                
            # Validate order_items structure
            if not isinstance(order_items, list):
                logger.warning(f"⚠️ Invalid order_items type: {type(order_items)}, converting to empty list")
                order_items = []
            else:
                # Sanitize order items
                sanitized_items = []
                for item in order_items[:20]:  # Limit number of items
                    if isinstance(item, dict):
                        sanitized_item = {
                            'name': str(item.get('name', ''))[:100],
                            'size': str(item.get('size', ''))[:50],
                            'price': float(item.get('price', 0)) if isinstance(item.get('price'), (int, float)) else 0
                        }
                        sanitized_items.append(sanitized_item)
                order_items = sanitized_items
                
        except Exception as e:
            logger.error(f"❌ Input validation error: {e}")
            return False
        
        # Database operation with retries
        max_retries = 3
        for attempt in range(max_retries):
            conn = None
            try:
                conn = self.get_connection()
                with conn:  # This ensures transaction is rolled back on exception
                    with conn.cursor() as cursor:
                        cursor.execute("""
                            INSERT INTO reviews (username, rating, review_text, review_date, 
                                               order_items, order_num, user_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """, (username, rating, review_text, review_date, 
                              psycopg2.extras.Json(order_items), order_num, user_id))
                        
                        logger.info(f"✅ Review saved for user {user_id}, order {order_num} (attempt {attempt + 1})")
                        return True
                        
            except psycopg2.IntegrityError as e:
                logger.error(f"❌ Database integrity error saving review: {e}")
                return False  # Don't retry integrity errors
            except (psycopg2.OperationalError, psycopg2.DatabaseError) as e:
                logger.error(f"❌ Database error saving review (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue
                return False
            except Exception as e:
                logger.error(f"❌ Unexpected error saving review (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(1)
                    continue
                return False
            finally:
                if conn:
                    try:
                        self.put_connection(conn)
                    except:
                        pass
                        
        return False
    
    def get_all_reviews(self, limit: int = 50, offset: int = 0) -> List[Dict]:
        """Get all reviews from the database with enhanced error handling and pagination"""
        
        # Validate limit parameter
        try:
            limit = int(limit)
            if limit <= 0 or limit > 1000:  # Reasonable bounds
                limit = 50
        except (ValueError, TypeError):
            limit = 50
        
        # Validate offset parameter
        try:
            offset = int(offset)
            if offset < 0:
                offset = 0
        except (ValueError, TypeError):
            offset = 0
            
        # Database operation with retries
        max_retries = 3
        for attempt in range(max_retries):
            conn = None
            try:
                conn = self.get_connection()
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT username, rating, review_text, review_date, 
                               order_items, order_num, user_id
                        FROM reviews 
                        ORDER BY created_at DESC 
                        LIMIT %s OFFSET %s
                    """, (limit, offset))
                    
                    reviews = cursor.fetchall()
                    result = []
                    
                    # Validate each review before adding to result
                    for review in reviews:
                        try:
                            review_dict = dict(review)
                            # Ensure all expected fields exist and are valid
                            validated_review = {
                                'username': str(review_dict.get('username', 'Unknown'))[:255],
                                'rating': max(1, min(5, int(review_dict.get('rating', 5)))),
                                'review_text': str(review_dict.get('review_text', ''))[:2000],
                                'review_date': review_dict.get('review_date'),
                                'order_items': review_dict.get('order_items', []) or [],
                                'order_num': str(review_dict.get('order_num', 'N/A'))[:50],
                                'user_id': review_dict.get('user_id', 0)
                            }
                            result.append(validated_review)
                        except Exception as e:
                            logger.warning(f"⚠️ Skipping invalid review: {e}")
                            continue
                    
                    logger.info(f"✅ Retrieved {len(result)} reviews (attempt {attempt + 1})")
                    return result
                    
            except (psycopg2.OperationalError, psycopg2.DatabaseError) as e:
                logger.error(f"❌ Database error getting reviews (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(2 ** attempt)
                    continue
            except Exception as e:
                logger.error(f"❌ Unexpected error getting reviews (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(1)
                    continue
            finally:
                if conn:
                    try:
                        self.put_connection(conn)
                    except:
                        pass
                        
        logger.error("❌ Failed to get reviews after all retry attempts")
        return []
    
    def user_has_reviewed_order(self, user_id: int, order_num: str) -> bool:
        """Check if user has already reviewed a specific order"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) as count 
                    FROM reviews 
                    WHERE user_id = %s AND order_num = %s
                """, (user_id, order_num))
                
                result = cursor.fetchone()
                if result and isinstance(result, dict):
                    return result.get('count', 0) > 0
                elif result and isinstance(result, (tuple, list)) and len(result) > 0:
                    return result[0] > 0
                return False
                
        except Exception as e:
            logger.error(f"❌ Error checking if user reviewed order: {e}")
            return False
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def get_review_count(self) -> int:
        """Get total number of reviews"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) as count FROM reviews")
                result = cursor.fetchone()
                if result and isinstance(result, dict):
                    return result.get('count', 0)
                elif result and isinstance(result, (tuple, list)) and len(result) > 0:
                    return result[0]
                return 0
                
        except Exception as e:
            logger.error(f"❌ Error getting review count: {e}")
            return 0
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def get_total_orders_count(self) -> int:
        """Get total number of ALL orders (pending + confirmed) - starts from 106"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # Count ALL orders (pending + confirmed), not just confirmed
                cursor.execute("SELECT COUNT(*) as count FROM orders")
                result = cursor.fetchone()
                count = 0
                if result and isinstance(result, dict):
                    count = result.get('count', 0)
                elif result and isinstance(result, (tuple, list)) and len(result) > 0:
                    count = result[0]
                
                # Start counting from 106 (base number)
                # This ensures the counter only goes up and never down
                return 106 + count
                
        except Exception as e:
            logger.error(f"❌ Error getting total orders count: {e}")
            return 106  # Return base number on error
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def get_user_orders(self, user_id: int, limit: int = 10) -> List[Dict]:
        """Get user's order history from database"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT order_num, user_id, username, status, order_details, created_at, confirmed_at
                    FROM orders 
                    WHERE user_id = %s 
                    ORDER BY created_at DESC 
                    LIMIT %s
                """, (user_id, limit))
                
                orders = cursor.fetchall()
                result = []
                
                for order in orders:
                    order_dict = dict(order)
                    
                    # Extract payment method and total from JSON details
                    details_json = order_dict.get('order_details')
                    
                    # Handle NULL or non-dict values
                    if not details_json:
                        details_json = {}
                    elif isinstance(details_json, str):
                        import json
                        try:
                            details_json = json.loads(details_json)
                        except:
                            details_json = {}
                    
                    details_text = details_json.get('details', '') if isinstance(details_json, dict) else ''
                    
                    # Extract total price
                    import re
                    total_match = re.search(r'Total:\s*£([\d.]+)', details_text)
                    if total_match:
                        order_dict['total_price'] = float(total_match.group(1))
                    else:
                        order_dict['total_price'] = 0.0
                    
                    # Extract payment method
                    if 'PayPal' in details_text:
                        order_dict['payment_method'] = 'PayPal'
                    elif 'Litecoin' in details_text or 'LTC' in details_text:
                        order_dict['payment_method'] = 'Litecoin'
                    else:
                        order_dict['payment_method'] = 'Unknown'
                    
                    result.append(order_dict)
                
                logger.info(f"✅ Retrieved {len(result)} orders for user {user_id}")
                return result
                
        except Exception as e:
            logger.error(f"❌ Error getting user orders: {e}")
            return []
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def get_sales_analytics(self, days: int = 30) -> Dict:
        """Get sales analytics for the last N days"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # Get confirmed orders from last N days
                # Use parameterized query to prevent SQL injection
                cursor.execute("""
                    SELECT order_details, created_at, confirmed_at
                    FROM orders 
                    WHERE status = 'confirmed' 
                    AND created_at >= NOW() - make_interval(days => %s)
                    ORDER BY created_at DESC
                """, (days,))
                
                orders = cursor.fetchall()
                
                total_revenue = 0
                product_sales = {}
                payment_methods = {}
                
                for order in orders:
                    order_details = order['order_details']
                    if isinstance(order_details, str):
                        try:
                            order_details = json.loads(order_details)
                        except:
                            continue
                    
                    # Parse order details (handles both structured and text format)
                    import re
                    
                    # Extract revenue
                    total = 0
                    if 'total' in order_details:
                        total = order_details.get('total', 0)
                    elif 'details' in order_details:
                        # Parse text format: "💰 *Final Total:* £180.00"
                        details_text = order_details.get('details', '')
                        total_match = re.search(r'Final Total.*?£([\d,]+\.?\d*)', details_text)
                        if total_match:
                            total = float(total_match.group(1).replace(',', ''))
                    
                    total_revenue += total
                    
                    # Track product sales
                    items = []
                    if 'items' in order_details:
                        items = order_details.get('items', [])
                    elif 'details' in order_details:
                        # Parse items from text format: "1. Product Name (size) - £price"
                        details_text = order_details.get('details', '')
                        item_matches = re.findall(r'\d+\.\s+(.+?)\s+\([^)]+\)\s+-\s+£', details_text)
                        items = [{'name': item_name.strip()} for item_name in item_matches]
                    
                    for item in items:
                        product_name = item.get('name', 'Unknown')
                        if product_name in product_sales:
                            product_sales[product_name] += 1
                        else:
                            product_sales[product_name] = 1
                    
                    # Track payment methods
                    payment_method = 'Unknown'
                    if 'payment_method' in order_details:
                        payment_method = order_details.get('payment_method', 'Unknown')
                    elif 'details' in order_details:
                        # Parse payment from text: "💳 *Payment:* PayPal"
                        details_text = order_details.get('details', '')
                        payment_match = re.search(r'Payment:\*\s+(.+?)\n', details_text)
                        if payment_match:
                            payment_method = payment_match.group(1).strip()
                    
                    if payment_method in payment_methods:
                        payment_methods[payment_method] += 1
                    else:
                        payment_methods[payment_method] = 1
                
                # Sort products by sales
                top_products = sorted(product_sales.items(), key=lambda x: x[1], reverse=True)[:5]
                
                logger.info(f"✅ Retrieved sales analytics for last {days} days")
                return {
                    'total_revenue': total_revenue,
                    'total_orders': len(orders),
                    'top_products': top_products,
                    'payment_methods': payment_methods
                }
                
        except Exception as e:
            logger.error(f"❌ Error getting sales analytics: {e}")
            return {
                'total_revenue': 0,
                'total_orders': 0,
                'top_products': [],
                'payment_methods': {}
            }
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def get_next_order_number(self) -> str:
        """Generate random 5-digit order number"""
        import random
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # Keep trying until we find a unique random order number
                max_attempts = 50
                for _ in range(max_attempts):
                    # Generate random 5-digit number
                    random_num = random.randint(10000, 99999)
                    order_num = f"ORD{random_num}"
                    
                    # Check if this order number already exists
                    cursor.execute("""
                        SELECT COUNT(*) as count FROM orders WHERE order_num = %s
                    """, (order_num,))
                    result = cursor.fetchone()
                    
                    count = 0
                    if result and isinstance(result, dict):
                        count = result.get('count', 0)
                    elif result and isinstance(result, (tuple, list)) and len(result) > 0:
                        count = result[0]
                    
                    # If unique, return it
                    if count == 0:
                        return order_num
                
                # Fallback if somehow all attempts failed
                return f"ORD{random.randint(10000, 99999)}"
                
        except Exception as e:
            logger.error(f"❌ Error generating order number: {e}")
            # Fallback to random number
            return f"ORD{random.randint(10000, 99999)}"
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def get_review_statistics(self) -> dict:
        """Get review statistics including total and average rating"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_reviews,
                        COALESCE(AVG(rating), 0) as average_rating
                    FROM reviews
                """)
                result = cursor.fetchone()
                
                if result:
                    if isinstance(result, dict):
                        return {
                            'total_reviews': result.get('total_reviews', 0),
                            'average_rating': float(result.get('average_rating', 0))
                        }
                    elif isinstance(result, (tuple, list)) and len(result) >= 2:
                        return {
                            'total_reviews': result[0] or 0,
                            'average_rating': float(result[1] or 0)
                        }
                
                return {'total_reviews': 0, 'average_rating': 0.0}
                
        except Exception as e:
            logger.error(f"❌ Error getting review statistics: {e}")
            return {'total_reviews': 0, 'average_rating': 0.0}
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    # ====================== DISCOUNT SYSTEM METHODS ======================
    
    def create_promotion(self, promo_type: str, name: str, description: str = "", 
                        target_products: List[str] = None, percent_off: float = 0, 
                        amount_off: float = 0, buy_qty: int = 1, get_qty: int = 0,
                        min_spend: float = 0, end_at=None, stackable: bool = False) -> bool:
        """Create a new promotion"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO promotions (type, name, description, target_product_ids, 
                                          percent_off, amount_off, buy_qty, get_qty, 
                                          min_spend, end_at, stackable)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (promo_type, name, description, json.dumps(target_products or []),
                     percent_off, amount_off, buy_qty, get_qty, min_spend, end_at, stackable))
                conn.commit()
                logger.info(f"✅ Created promotion: {name}")
                return True
        except Exception as e:
            logger.error(f"❌ Error creating promotion: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def get_active_promotions(self) -> List[Dict]:
        """Get all active promotions"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM promotions 
                    WHERE active = TRUE 
                    AND (end_at IS NULL OR end_at > CURRENT_TIMESTAMP)
                    ORDER BY created_at DESC
                """)
                promotions = []
                for row in cursor.fetchall():
                    promo = dict(row)
                    
                    # Safe JSON parsing - handle both string and already-parsed data
                    if isinstance(promo.get('target_product_ids'), str):
                        promo['target_product_ids'] = json.loads(promo['target_product_ids'] or '[]')
                    elif promo.get('target_product_ids') is None:
                        promo['target_product_ids'] = []
                    
                    if isinstance(promo.get('segments'), str):
                        promo['segments'] = json.loads(promo['segments'] or '[]')
                    elif promo.get('segments') is None:
                        promo['segments'] = []
                    
                    promotions.append(promo)
                return promotions
        except Exception as e:
            logger.error(f"❌ Error getting active promotions: {e}")
            return []
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def create_coupon(self, code: str, coupon_type: str, value: float, 
                     min_spend: float = 0, expires_at=None, max_uses: int = 1000,
                     per_user_limit: int = 1):
        """Create a new coupon code"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # Check if coupon already exists
                cursor.execute("SELECT code FROM coupons WHERE UPPER(code) = UPPER(%s)", (code,))
                existing = cursor.fetchone()
                if existing:
                    return {"success": False, "error": f"Coupon code '{code}' already exists"}
                
                # Create new coupon
                cursor.execute("""
                    INSERT INTO coupons (code, type, value, min_spend, expires_at, max_uses, per_user_limit)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (code.upper(), coupon_type, value, min_spend, expires_at, max_uses, per_user_limit))
                conn.commit()
                logger.info(f"✅ Created coupon: {code}")
                return {"success": True, "message": f"Coupon '{code}' created successfully"}
        except Exception as e:
            logger.error(f"❌ Error creating coupon: {e}")
            if conn:
                conn.rollback()
            return {"success": False, "error": "Database error occurred"}
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def validate_coupon(self, code: str, user_id: int, cart_total: float) -> Dict:
        """Validate a coupon code for a user"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # Get coupon details
                cursor.execute("""
                    SELECT * FROM coupons 
                    WHERE code = %s AND active = TRUE 
                    AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
                """, (code.upper(),))
                
                coupon = cursor.fetchone()
                if not coupon:
                    return {"valid": False, "message": "Invalid or expired coupon code"}
                
                # Check usage limits
                if coupon['current_uses'] >= coupon['max_uses']:
                    return {"valid": False, "message": "Coupon usage limit reached"}
                
                # Check minimum spend
                if cart_total < coupon['min_spend']:
                    return {"valid": False, "message": f"Minimum spend £{coupon['min_spend']:.2f} required"}
                
                # Check per-user limit
                cursor.execute("""
                    SELECT COUNT(*) FROM coupon_usage 
                    WHERE coupon_code = %s AND user_id = %s
                """, (code.upper(), user_id))
                
                user_usage = cursor.fetchone()[0]
                if user_usage >= coupon['per_user_limit']:
                    return {"valid": False, "message": "You have already used this coupon"}
                
                return {"valid": True, "coupon": dict(coupon)}
                
        except Exception as e:
            logger.error(f"❌ Error validating coupon: {e}")
            return {"valid": False, "message": "Error validating coupon"}
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def use_coupon(self, code: str, user_id: int, order_num: str) -> bool:
        """Record coupon usage"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # Record usage
                cursor.execute("""
                    INSERT INTO coupon_usage (coupon_code, user_id, order_num)
                    VALUES (%s, %s, %s)
                """, (code.upper(), user_id, order_num))
                
                # Update usage count
                cursor.execute("""
                    UPDATE coupons SET current_uses = current_uses + 1
                    WHERE code = %s
                """, (code.upper(),))
                
                conn.commit()
                logger.info(f"✅ Coupon {code} used by user {user_id}")
                return True
        except Exception as e:
            logger.error(f"❌ Error using coupon: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def get_all_promotions(self) -> List[Dict]:
        """Get all promotions for admin panel"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM promotions ORDER BY created_at DESC
                """)
                promotions = []
                for row in cursor.fetchall():
                    promo = dict(row)
                    
                    # Safe JSON parsing - handle both string and already-parsed data
                    if isinstance(promo.get('target_product_ids'), str):
                        promo['target_product_ids'] = json.loads(promo['target_product_ids'] or '[]')
                    elif promo.get('target_product_ids') is None:
                        promo['target_product_ids'] = []
                    
                    if isinstance(promo.get('segments'), str):
                        promo['segments'] = json.loads(promo['segments'] or '[]')
                    elif promo.get('segments') is None:
                        promo['segments'] = []
                    
                    promotions.append(promo)
                return promotions
        except Exception as e:
            logger.error(f"❌ Error getting all promotions: {e}")
            return []
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def toggle_promotion(self, promo_id: int) -> bool:
        """Toggle promotion active status"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE promotions SET active = NOT active WHERE id = %s
                """, (promo_id,))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"❌ Error toggling promotion: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def update_promotion_status(self, promo_id: int, active: bool) -> bool:
        """Update promotion active status to specific value"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE promotions SET active = %s WHERE id = %s
                """, (active, promo_id))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"❌ Error updating promotion status: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def validate_coupon(self, code: str, user_id: str) -> Dict:
        """Validate a coupon code and return coupon details if valid"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT * FROM coupons 
                    WHERE UPPER(code) = UPPER(%s) 
                    AND active = TRUE 
                    AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
                    AND current_uses < max_uses
                """, (code,))
                
                row = cursor.fetchone()
                if not row:
                    return None
                
                coupon = dict(row)
                
                # Check per-user limit if applicable
                if coupon.get('per_user_limit', 1) > 0:
                    cursor.execute("""
                        SELECT COUNT(*) as usage_count FROM coupon_usage 
                        WHERE coupon_code = %s AND user_id = %s
                    """, (coupon['code'], user_id))
                    
                    usage_row = cursor.fetchone()
                    if usage_row and usage_row['usage_count'] >= coupon['per_user_limit']:
                        return None  # User has exceeded their limit
                
                return coupon
                
        except Exception as e:
            logger.error(f"❌ Error validating coupon: {e}")
            return None
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def get_reviews_for_product(self, product_name: str, limit: int = 5) -> List[Dict]:
        """Get reviews for a specific product using efficient JSONB query"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT username, rating, review_text, review_date, 
                           order_items, order_num, user_id
                    FROM reviews 
                    WHERE EXISTS (
                        SELECT 1 FROM jsonb_array_elements(order_items) elem 
                        WHERE elem->>'name' = %s
                    )
                    ORDER BY created_at DESC 
                    LIMIT %s
                """, (product_name, limit))
                
                reviews = cursor.fetchall()
                return [dict(review) for review in reviews]
                
        except Exception as e:
            logger.error(f"❌ Error getting reviews for product {product_name}: {e}")
            return []
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def migrate_existing_reviews(self, existing_reviews: List[tuple]):
        """Migrate reviews from the old in-memory format to database"""
        migrated_count = 0
        conn = None
        
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                for review in existing_reviews:
                    try:
                        # Handle different review formats
                        if len(review) >= 6:
                            # New format: (username, rating, review_text, date, order_items, order_num)
                            username, rating, review_text, date, order_items, order_num = review[:6]
                            user_id = 0  # Default for migrated reviews
                        else:
                            # Old format: (username, rating, review_text, date)
                            username, rating, review_text, date = review[:4]
                            order_items = []
                            order_num = "MIGRATED"
                            user_id = 0
                        
                        cursor.execute("""
                            INSERT INTO reviews (username, rating, review_text, review_date, 
                                               order_items, order_num, user_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING
                        """, (username, rating, review_text, date, 
                              psycopg2.extras.Json(order_items), order_num, user_id))
                        
                        migrated_count += 1
                        
                    except Exception as e:
                        logger.error(f"❌ Error migrating individual review: {e}")
                        continue
                
                conn.commit()
                logger.info(f"✅ Migrated {migrated_count} existing reviews to database")
                
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            logger.error(f"❌ Error during migration: {e}")
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
        
        return migrated_count
    
    def save_order(self, order_num: str, user_id: int, username: str, order_details: str) -> bool:
        """Save order details for admin confirmation and tracking"""
        try:
            import json
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cursor:
                    # Save to order_confirmations table for admin
                    cursor.execute("""
                        INSERT INTO order_confirmations (order_num, user_id, username, order_details)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (order_num) DO UPDATE SET
                            user_id = EXCLUDED.user_id,
                            username = EXCLUDED.username,
                            order_details = EXCLUDED.order_details
                    """, (order_num, user_id, username, order_details))
                    
                    # Convert order_details text to JSON for orders table
                    order_json = json.dumps({"details": order_details})
                    
                    # Also save to orders table for tracking
                    cursor.execute("""
                        INSERT INTO orders (order_num, user_id, username, status, order_details)
                        VALUES (%s, %s, %s, 'pending', %s::jsonb)
                        ON CONFLICT (order_num) DO UPDATE SET
                            user_id = EXCLUDED.user_id,
                            username = EXCLUDED.username,
                            order_details = EXCLUDED.order_details
                    """, (order_num, user_id, username, order_json))
                    
                logger.info(f"✅ Order {order_num} saved to both tables for user {user_id}")
                return True
        except Exception as e:
            logger.error(f"❌ Error saving order: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def get_pending_orders(self) -> List[Dict]:
        """Get all orders pending admin confirmation"""
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT order_num, user_id, username, order_details, created_at
                    FROM order_confirmations 
                    WHERE confirmed_by_admin = FALSE
                    ORDER BY created_at DESC
                """)
                
                orders = cursor.fetchall()
                return [dict(order) for order in orders]
        except Exception as e:
            logger.error(f"❌ Error getting pending orders: {e}")
            return []
    
    def get_confirmed_orders(self, limit: int = 50) -> List[Dict]:
        """Get all confirmed orders"""
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT order_num, user_id, username, order_details, 
                           created_at, confirmation_date
                    FROM order_confirmations 
                    WHERE confirmed_by_admin = TRUE
                    ORDER BY confirmation_date DESC
                    LIMIT %s
                """, (limit,))
                
                orders = cursor.fetchall()
                return [dict(order) for order in orders]
        except Exception as e:
            logger.error(f"❌ Error getting confirmed orders: {e}")
            return []
    
    def cleanup_old_confirmed_orders(self, keep_last: int = 30) -> int:
        """Delete old confirmed orders, keeping only the last N orders"""
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        DELETE FROM order_confirmations
                        WHERE id NOT IN (
                            SELECT id FROM order_confirmations
                            WHERE confirmed_by_admin = TRUE
                            ORDER BY confirmation_date DESC
                            LIMIT %s
                        )
                        AND confirmed_by_admin = TRUE
                    """, (keep_last,))
                    
                    deleted_count = cursor.rowcount
                    logger.info(f"✅ Cleaned up {deleted_count} old confirmed orders, keeping last {keep_last}")
                    return deleted_count
        except Exception as e:
            logger.error(f"❌ Error cleaning up old confirmed orders: {e}")
            return 0
    
    def confirm_order_by_admin(self, order_num: str) -> Optional[Dict]:
        """Confirm an order and return customer details for notification"""
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cursor:
                    # Update order_confirmations table
                    cursor.execute("""
                        UPDATE order_confirmations 
                        SET confirmed_by_admin = TRUE, confirmation_date = CURRENT_TIMESTAMP
                        WHERE order_num = %s AND confirmed_by_admin = FALSE
                        RETURNING user_id, username, order_details
                    """, (order_num,))
                    
                    result = cursor.fetchone()
                    if result:
                        # CRITICAL: Also update orders table to 'confirmed' status
                        # This is what makes the order count in total_orders counter
                        cursor.execute("""
                            UPDATE orders
                            SET status = 'confirmed', confirmed_at = CURRENT_TIMESTAMP
                            WHERE order_num = %s
                        """, (order_num,))
                        
                        logger.info(f"✅ Order {order_num} confirmed by admin - status updated to confirmed")
                        return dict(result)
                    else:
                        logger.warning(f"⚠️ Order {order_num} not found or already confirmed")
                        return None
        except Exception as e:
            logger.error(f"❌ Error confirming order: {e}")
            return None
    
    def get_order_by_number(self, order_num: int | str) -> Optional[Dict]:
        """Get a specific order by order number - handles both '26974' and 'ORD26974' formats"""
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # Convert to string and handle both formats
                order_str = str(order_num)
                
                # If user entered just a number, try with ORD prefix first
                if order_str.isdigit():
                    order_with_prefix = f"ORD{order_str}"
                    # Try with ORD prefix first
                    cursor.execute("""
                        SELECT order_num, user_id, username, status, order_details, created_at, confirmed_at
                        FROM orders 
                        WHERE order_num = %s OR order_num = %s
                    """, (order_with_prefix, order_str))
                else:
                    # User entered with prefix, search as-is
                    cursor.execute("""
                        SELECT order_num, user_id, username, status, order_details, created_at, confirmed_at
                        FROM orders 
                        WHERE order_num = %s
                    """, (order_str,))
                
                result = cursor.fetchone()
                if result:
                    order_dict = dict(result)
                    
                    # Extract payment method and total from JSON details
                    details_json = order_dict.get('order_details')
                    
                    # Handle NULL or non-dict values
                    if not details_json:
                        details_json = {}
                    elif isinstance(details_json, str):
                        import json
                        try:
                            details_json = json.loads(details_json)
                        except:
                            details_json = {}
                    
                    # Parse details text if it's in the format
                    details_text = details_json.get('details', '') if isinstance(details_json, dict) else ''
                    
                    # Extract total price from details text
                    import re
                    total_match = re.search(r'Total:\s*£([\d.]+)', details_text)
                    if total_match:
                        order_dict['total_price'] = float(total_match.group(1))
                    else:
                        order_dict['total_price'] = 0.0
                    
                    # Extract payment method
                    if 'PayPal' in details_text:
                        order_dict['payment_method'] = 'PayPal'
                    elif 'Litecoin' in details_text or 'LTC' in details_text:
                        order_dict['payment_method'] = 'Litecoin'
                    else:
                        order_dict['payment_method'] = 'Unknown'
                    
                    return order_dict
                return None
        except Exception as e:
            logger.error(f"❌ Error getting order by number: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None
    
    def get_all_orders(self, limit: int = 1000) -> List[Dict]:
        """Get all orders with limit"""
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT order_num, user_id, username, status, order_details, created_at, confirmed_at
                    FROM orders 
                    ORDER BY created_at DESC 
                    LIMIT %s
                """, (limit,))
                
                orders = cursor.fetchall()
                result = []
                
                for order in orders:
                    order_dict = dict(order)
                    
                    # Extract payment method and total from JSON details
                    details_json = order_dict.get('order_details')
                    
                    # Handle NULL or non-dict values
                    if not details_json:
                        details_json = {}
                    elif isinstance(details_json, str):
                        import json
                        try:
                            details_json = json.loads(details_json)
                        except:
                            details_json = {}
                    
                    details_text = details_json.get('details', '') if isinstance(details_json, dict) else ''
                    
                    # Extract total price
                    import re
                    total_match = re.search(r'Total:\s*£([\d.]+)', details_text)
                    if total_match:
                        order_dict['total_price'] = float(total_match.group(1))
                    else:
                        order_dict['total_price'] = 0.0
                    
                    # Extract payment method
                    if 'PayPal' in details_text:
                        order_dict['payment_method'] = 'PayPal'
                    elif 'Litecoin' in details_text or 'LTC' in details_text:
                        order_dict['payment_method'] = 'Litecoin'
                    else:
                        order_dict['payment_method'] = 'Unknown'
                    
                    result.append(order_dict)
                
                return result
        except Exception as e:
            logger.error(f"❌ Error getting all orders: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return []
    
    def delete_order(self, order_num: int) -> bool:
        """Delete an order completely (admin only)"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # Delete from orders table
                cursor.execute("DELETE FROM orders WHERE order_num = %s", (order_num,))
                # Delete from order_confirmations table
                cursor.execute("DELETE FROM order_confirmations WHERE order_num = %s", (order_num,))
                conn.commit()
                logger.info(f"✅ Order {order_num} deleted successfully")
                return True
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"❌ Error deleting order {order_num}: {e}")
            return False
        finally:
            if conn:
                self.put_connection(conn)
    
    def block_user(self, user_id: int, username: str | None = None, blocked_by: str = "admin", reason: str | None = None) -> bool:
        """Block a user from using the bot"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO blocked_users (user_id, username, blocked_by, reason)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE 
                    SET username = EXCLUDED.username,
                        blocked_by = EXCLUDED.blocked_by,
                        reason = EXCLUDED.reason,
                        blocked_at = CURRENT_TIMESTAMP
                """, (user_id, username, blocked_by, reason))
                conn.commit()
                logger.info(f"✅ User {user_id} blocked successfully")
                return True
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"❌ Error blocking user {user_id}: {e}")
            return False
        finally:
            if conn:
                self.put_connection(conn)
    
    def unblock_user(self, user_id: int) -> bool:
        """Unblock a user"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("DELETE FROM blocked_users WHERE user_id = %s", (user_id,))
                conn.commit()
                logger.info(f"✅ User {user_id} unblocked successfully")
                return True
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"❌ Error unblocking user {user_id}: {e}")
            return False
        finally:
            if conn:
                self.put_connection(conn)
    
    def is_user_blocked(self, user_id: int) -> bool:
        """Check if a user is blocked"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM blocked_users WHERE user_id = %s", (user_id,))
                result = cursor.fetchone()
                return result is not None
        except Exception as e:
            logger.error(f"❌ Error checking if user {user_id} is blocked: {e}")
            return False
        finally:
            if conn:
                self.put_connection(conn)
    
    def get_blocked_users(self) -> List[Dict]:
        """Get all blocked users"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT user_id, username, blocked_by, reason, blocked_at
                    FROM blocked_users
                    ORDER BY blocked_at DESC
                """)
                results = cursor.fetchall()
                return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"❌ Error getting blocked users: {e}")
            return []
        finally:
            if conn:
                self.put_connection(conn)
    
    def acquire_bot_instance_lock(self, instance_id: str, lease_duration_minutes: int = 5) -> bool:
        """Acquire distributed lock for bot instance"""
        try:
            import socket
            import os
            from datetime import datetime, timedelta
            
            # Generate host info
            host_info = f"PID:{os.getpid()}, Host:{socket.gethostname()}"
            
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cursor:
                    # Clean up expired leases first
                    cursor.execute("DELETE FROM bot_instance WHERE lease_until < NOW()")
                    
                    # Try to insert new lock
                    try:
                        cursor.execute("""
                            INSERT INTO bot_instance (instance_id, lease_until, heartbeat_at, host_info)
                            VALUES (%s, NOW() + INTERVAL '%s minutes', NOW(), %s)
                        """, (instance_id, lease_duration_minutes, host_info))
                        
                        logger.info(f"✅ Bot instance lock acquired for {instance_id}")
                        return True
                        
                    except Exception as insert_error:
                        # Lock already exists, try to update if expired
                        if "duplicate key" in str(insert_error) or "already exists" in str(insert_error):
                            cursor.execute("""
                                UPDATE bot_instance 
                                SET lease_until = NOW() + INTERVAL '%s minutes',
                                    heartbeat_at = NOW(),
                                    host_info = %s
                                WHERE instance_id = %s AND lease_until < NOW()
                                RETURNING id
                            """, (lease_duration_minutes, host_info, instance_id))
                            
                            result = cursor.fetchone()
                            if result:
                                logger.info(f"✅ Bot instance lock acquired (renewed expired) for {instance_id}")
                                return True
                            else:
                                logger.warning(f"⚠️ Failed to acquire lock - another instance is active")
                                return False
                        else:
                            raise insert_error
                    
        except Exception as e:
            logger.error(f"❌ Error acquiring bot instance lock: {e}")
            return False
    
    def renew_bot_instance_lock(self, instance_id: str, lease_duration_minutes: int = 5) -> bool:
        """Renew the bot instance lock with heartbeat"""
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE bot_instance 
                        SET lease_until = CURRENT_TIMESTAMP + %s * INTERVAL '1 minute',
                            heartbeat_at = CURRENT_TIMESTAMP
                        WHERE instance_id = %s AND lease_until > CURRENT_TIMESTAMP
                        RETURNING id
                    """, (lease_duration_minutes, instance_id))
                    
                    result = cursor.fetchone()
                    renewed = result is not None
                    
                    if renewed:
                        logger.debug(f"🔄 Bot instance lock renewed for {instance_id}")
                    else:
                        logger.warning(f"⚠️ Failed to renew lock - lease may have expired")
                    
                    return renewed
                    
        except Exception as e:
            logger.error(f"❌ Error renewing bot instance lock: {e}")
            return False
    
    def release_bot_instance_lock(self, instance_id: str) -> bool:
        """Release the bot instance lock"""
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        DELETE FROM bot_instance 
                        WHERE instance_id = %s
                    """, (instance_id,))
                    
                    if cursor.rowcount > 0:
                        logger.info(f"✅ Bot instance lock released for {instance_id}")
                        return True
                    else:
                        logger.warning(f"⚠️ No lock found to release for {instance_id}")
                        return False
                    
        except Exception as e:
            logger.error(f"❌ Error releasing bot instance lock: {e}")
            return False
    
    def check_bot_instance_lock(self, instance_id: str) -> bool:
        """Check if this instance holds a valid lock"""
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT lease_until > CURRENT_TIMESTAMP as valid
                    FROM bot_instance 
                    WHERE instance_id = %s
                """, (instance_id,))
                
                result = cursor.fetchone()
                return result[0] if result else False
                
        except Exception as e:
            logger.error(f"❌ Error checking bot instance lock: {e}")
            return False
    
    # ====================== BROADCAST USER METHODS ======================
    
    def add_broadcast_user(self, user_id: int, username: str | None = None, 
                          first_name: str | None = None, last_name: str | None = None) -> bool:
        """Add or update a user for broadcast messaging"""
        conn = None
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO broadcast_users (user_id, username, first_name, last_name, last_seen)
                        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (user_id) 
                        DO UPDATE SET 
                            username = COALESCE(%s, broadcast_users.username),
                            first_name = COALESCE(%s, broadcast_users.first_name), 
                            last_name = COALESCE(%s, broadcast_users.last_name),
                            last_seen = CURRENT_TIMESTAMP,
                            active = TRUE
                    """, (user_id, username, first_name, last_name, username, first_name, last_name))
                    
                    logger.info(f"✅ Added/updated broadcast user: {user_id}")
                    return True
                    
        except Exception as e:
            logger.error(f"❌ Error adding broadcast user {user_id}: {e}")
            return False
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def get_broadcast_users(self) -> List[int]:
        """Get all active broadcast user IDs"""
        max_retries = 3
        for attempt in range(max_retries):
            conn = None
            try:
                conn = self.get_connection()
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT user_id FROM broadcast_users 
                        WHERE active = TRUE 
                        ORDER BY last_seen DESC
                    """)
                    
                    result = cursor.fetchall()
                    if result:
                        users = [int(row['user_id']) for row in result]
                        logger.info(f"✅ Retrieved {len(users)} active broadcast users (attempt {attempt + 1})")
                        return users
                    else:
                        logger.warning(f"⚠️ No active broadcast users found in database (attempt {attempt + 1})")
                        return []
                    
            except Exception as e:
                logger.exception(f"❌ Error getting broadcast users (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    import time
                    time.sleep(2 ** attempt)
                    continue
            finally:
                if conn:
                    try:
                        self.put_connection(conn)
                    except:
                        pass
        
        # If all attempts failed, return empty list but log critical error
        logger.critical("❌ CRITICAL: Failed to get broadcast users after all retry attempts")
        return []
    
    def get_all_broadcast_users(self) -> List[Dict]:
        """Get all active broadcast users with full details"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT user_id, username, first_name, last_name, last_seen, first_interaction
                    FROM broadcast_users 
                    WHERE active = TRUE 
                    ORDER BY last_seen DESC
                """)
                
                rows = cursor.fetchall()
                # Convert RealDictRow objects to regular dicts
                return [dict(row) for row in rows]
                
        except Exception as e:
            logger.error(f"❌ Error getting all broadcast users: {e}")
            return []
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def get_user_info(self, user_id: int) -> Dict:
        """Get a single user's information by user_id"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT user_id, username, first_name, last_name, last_seen, first_interaction
                    FROM broadcast_users 
                    WHERE user_id = %s
                """, (user_id,))
                
                row = cursor.fetchone()
                if row:
                    return dict(row)
                return None
                
        except Exception as e:
            logger.error(f"❌ Error getting user info for {user_id}: {e}")
            return None
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def restore_all_users_from_history(self) -> int:
        """BULLETPROOF: Restore all users who have EVER interacted with bot"""
        conn = None
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cursor:
                    # Find ALL users who have ever interacted with the bot
                    cursor.execute("""
                        INSERT INTO broadcast_users (user_id, first_interaction, last_seen, active) 
                        SELECT DISTINCT user_id, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE
                        FROM (
                          SELECT user_id FROM order_confirmations 
                          UNION 
                          SELECT user_id FROM reviews 
                          UNION 
                          SELECT user_id FROM broadcast_users
                        ) all_users
                        WHERE user_id IS NOT NULL AND user_id != 12345
                        ON CONFLICT (user_id) 
                        DO UPDATE SET 
                          active = TRUE,
                          last_seen = CURRENT_TIMESTAMP
                    """)
                    
                    restored_count = cursor.rowcount
                    logger.info(f"✅ BULLETPROOF RESTORE: Added/reactivated {restored_count} users from history")
                    return restored_count
                    
        except Exception as e:
            logger.error(f"❌ Error in bulletproof restore: {e}")
            return 0
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def get_broadcast_user_count(self) -> int:
        """Get count of active broadcast users"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM broadcast_users WHERE active = TRUE")
                result = cursor.fetchone()
                count = result[0] if result else 0
                logger.info(f"✅ Active broadcast users count: {count}")
                return count
                
        except Exception as e:
            logger.error(f"❌ Error getting broadcast user count: {e}")
            return 0
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def deactivate_broadcast_user(self, user_id: int) -> bool:
        """Deactivate a user from broadcast list (soft delete)"""
        conn = None
        try:
            conn = self.get_connection()
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE broadcast_users SET active = FALSE 
                        WHERE user_id = %s
                    """, (user_id,))
                    
                    if cursor.rowcount > 0:
                        logger.info(f"✅ Deactivated broadcast user: {user_id}")
                        return True
                    else:
                        logger.warning(f"⚠️ User {user_id} not found for deactivation")
                        return False
                    
        except Exception as e:
            logger.error(f"❌ Error deactivating broadcast user {user_id}: {e}")
            return False
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def acquire_single_instance_lock(self, instance_id: str, timeout_minutes: int = 5) -> bool:
        """Acquire a single-instance lock to prevent multiple bot instances"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # Create instance lock table if it doesn't exist
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS bot_instance_lock (
                        lock_name VARCHAR(50) PRIMARY KEY,
                        instance_id VARCHAR(100) NOT NULL,
                        acquired_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        expires_at TIMESTAMP NOT NULL
                    )
                """)
                
                # Try to acquire lock
                from datetime import datetime, timedelta
                expires_at = datetime.now() + timedelta(minutes=timeout_minutes)
                cursor.execute("""
                    INSERT INTO bot_instance_lock (lock_name, instance_id, expires_at) 
                    VALUES ('main_bot', %s, %s)
                    ON CONFLICT (lock_name) 
                    DO UPDATE SET 
                        instance_id = %s,
                        acquired_at = CURRENT_TIMESTAMP,
                        expires_at = %s
                    WHERE bot_instance_lock.expires_at < CURRENT_TIMESTAMP
                """, (instance_id, expires_at, instance_id, expires_at))
                
                # Check if we successfully acquired the lock
                cursor.execute(
                    "SELECT instance_id FROM bot_instance_lock WHERE lock_name = 'main_bot'"
                )
                result = cursor.fetchone()
                
                conn.commit()
                
                if result and result['instance_id'] == instance_id:
                    logger.info(f"✅ Acquired single-instance lock: {instance_id}")
                    return True
                else:
                    logger.warning(f"❌ Failed to acquire lock - another instance running: {result['instance_id'] if result else 'unknown'}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Error acquiring single-instance lock: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
                    
    def release_single_instance_lock(self, instance_id: str) -> bool:
        """Release the single-instance lock"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    DELETE FROM bot_instance_lock 
                    WHERE lock_name = 'main_bot' AND instance_id = %s
                """, (instance_id,))
                
                deleted_rows = cursor.rowcount
                conn.commit()
                
                if deleted_rows > 0:
                    logger.info(f"✅ Released single-instance lock: {instance_id}")
                    return True
                else:
                    logger.warning(f"⚠️ Lock not found for instance: {instance_id}")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Error releasing single-instance lock: {e}")
            if conn:
                conn.rollback()
            return False
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    # MENU MANAGEMENT METHODS
    
    def add_category(self, name: str, display_order: int = 0) -> dict:
        """Add a new menu category"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO menu_categories (name, display_order) 
                    VALUES (%s, %s) RETURNING id
                """, (name, display_order))
                category_id = cursor.fetchone()['id']
                conn.commit()
                logger.info(f"✅ Added category: {name}")
                return {"success": True, "id": category_id, "message": f"Category '{name}' created"}
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"❌ Error adding category: {e}")
            return {"success": False, "error": str(e)}
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def get_categories(self) -> list:
        """Get all active categories - returns list of dicts"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, name, display_order 
                    FROM menu_categories 
                    WHERE active = TRUE 
                    ORDER BY display_order, name
                """)
                rows = cursor.fetchall()
                # Convert RealDictRow objects to regular dicts to avoid serialization issues
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"❌ Error getting categories: {e}")
            return []
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def delete_category(self, category_name: str) -> dict:
        """Delete a category and all its products (soft delete)"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # Check if category exists
                cursor.execute("""
                    SELECT id FROM menu_categories WHERE name = %s AND active = TRUE
                """, (category_name,))
                result = cursor.fetchone()
                
                if not result:
                    return {"success": False, "error": f"Category '{category_name}' not found"}
                
                category_id = result['id']
                
                # Soft delete all products in this category
                cursor.execute("""
                    UPDATE menu_products 
                    SET active = FALSE 
                    WHERE category_id = %s
                """, (category_id,))
                
                # Soft delete the category
                cursor.execute("""
                    UPDATE menu_categories 
                    SET active = FALSE 
                    WHERE id = %s
                """, (category_id,))
                
                conn.commit()
                logger.info(f"✅ Deleted category: {category_name}")
                return {"success": True, "message": f"Category '{category_name}' and all its products deleted"}
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"❌ Error deleting category: {e}")
            return {"success": False, "error": str(e)}
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def add_product(self, category_id: int, name: str, description: str = None) -> dict:
        """Add a new product to a category"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO menu_products (category_id, name, description) 
                    VALUES (%s, %s, %s) RETURNING id
                """, (category_id, name, description))
                product_id = cursor.fetchone()['id']
                conn.commit()
                logger.info(f"✅ Added product: {name}")
                return {"success": True, "id": product_id, "message": f"Product '{name}' added"}
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"❌ Error adding product: {e}")
            return {"success": False, "error": str(e)}
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def add_pricing_tier(self, product_id: int, size: str, price: float, display_order: int = 0) -> dict:
        """Add a pricing tier to a product"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO menu_pricing (product_id, size, price, display_order) 
                    VALUES (%s, %s, %s, %s) RETURNING id
                """, (product_id, size, price, display_order))
                tier_id = cursor.fetchone()['id']
                conn.commit()
                logger.info(f"✅ Added pricing tier: {size} - £{price}")
                return {"success": True, "id": tier_id, "message": f"Pricing tier '{size}' - £{price} added"}
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"❌ Error adding pricing tier: {e}")
            return {"success": False, "error": str(e)}
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def update_product_pricing(self, category: str, product_name: str, pricing_tiers: List[Tuple[str, float]]) -> Dict:
        """Update pricing for an existing product"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # Get product ID - removed active check to allow updating any product
                cursor.execute("""
                    SELECT p.id 
                    FROM menu_products p
                    JOIN menu_categories c ON p.category_id = c.id
                    WHERE c.name = %s AND p.name = %s
                """, (category, product_name))
                
                result = cursor.fetchone()
                if not result:
                    logger.error(f"❌ Product not found: {product_name} in {category}")
                    return {'success': False, 'error': f'Product "{product_name}" not found in category "{category}"'}
                
                product_id = result['id']
                
                # Delete existing pricing tiers
                cursor.execute("""
                    DELETE FROM menu_pricing WHERE product_id = %s
                """, (product_id,))
                
                # Add new pricing tiers
                for tier_order, (size, price) in enumerate(pricing_tiers):
                    cursor.execute("""
                        INSERT INTO menu_pricing (product_id, size, price, display_order) 
                        VALUES (%s, %s, %s, %s)
                    """, (product_id, size, float(price), tier_order))
                
                # Ensure product is active after adding pricing
                cursor.execute("""
                    UPDATE menu_products SET active = TRUE WHERE id = %s
                """, (product_id,))
                
                conn.commit()
                logger.info(f"✅ Updated pricing for {product_name} in {category}")
                return {'success': True}
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"❌ Error updating product pricing: {e}")
            return {'success': False, 'error': str(e)}
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def rename_product(self, category: str, old_name: str, new_name: str) -> Dict:
        """Rename a product"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # Get product ID - removed active check for admin operations
                cursor.execute("""
                    SELECT p.id 
                    FROM menu_products p
                    JOIN menu_categories c ON p.category_id = c.id
                    WHERE c.name = %s AND p.name = %s
                """, (category, old_name))
                
                result = cursor.fetchone()
                if not result:
                    return {'success': False, 'error': f'Product "{old_name}" not found in category "{category}"'}
                
                product_id = result['id']
                
                # Update product name
                cursor.execute("""
                    UPDATE menu_products 
                    SET name = %s 
                    WHERE id = %s
                """, (new_name, product_id))
                
                conn.commit()
                logger.info(f"✅ Renamed product from {old_name} to {new_name} in {category}")
                return {'success': True}
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"❌ Error renaming product: {e}")
            return {'success': False, 'error': str(e)}
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def get_dynamic_menu(self) -> dict:
        """Get complete menu structure from database including descriptions"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # Get all categories with products and pricing
                cursor.execute("""
                    SELECT 
                        c.name as category_name,
                        p.name as product_name,
                        p.description as product_description,
                        pr.size,
                        pr.price
                    FROM menu_categories c
                    LEFT JOIN menu_products p ON c.id = p.category_id AND p.active = TRUE
                    LEFT JOIN menu_pricing pr ON p.id = pr.product_id AND pr.active = TRUE
                    WHERE c.active = TRUE
                    ORDER BY c.display_order, c.name, p.name, pr.display_order, pr.size
                """)
                
                rows = cursor.fetchall()
                menu = {}
                descriptions = {}  # Store descriptions separately
                
                for row in rows:
                    category = row['category_name']
                    product = row['product_name']
                    description = row['product_description']
                    size = row['size']
                    price = row['price']
                    
                    if category not in menu:
                        menu[category] = {}
                    
                    # Store description if product exists and has one
                    if product and description:
                        descriptions[product] = description
                    
                    if product and size and price:
                        if product not in menu[category]:
                            menu[category][product] = []
                        menu[category][product].append((size, int(price)))
                
                logger.info(f"✅ Loaded dynamic menu: {len(menu)} categories, {len(descriptions)} descriptions")
                return {'menu': menu, 'descriptions': descriptions}
                
        except Exception as e:
            logger.error(f"❌ Error getting dynamic menu: {e}")
            return {'menu': {}, 'descriptions': {}}
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def remove_product(self, category_name: str, product_name: str) -> dict:
        """Remove a product (soft delete)"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE menu_products SET active = FALSE 
                    WHERE name = %s AND category_id = (
                        SELECT id FROM menu_categories WHERE name = %s
                    )
                """, (product_name, category_name))
                
                if cursor.rowcount > 0:
                    conn.commit()
                    logger.info(f"✅ Removed product: {product_name}")
                    return {"success": True, "message": f"Product '{product_name}' removed"}
                else:
                    return {"success": False, "error": "Product not found"}
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"❌ Error removing product: {e}")
            return {"success": False, "error": str(e)}
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def migrate_existing_products(self, products_dict: dict):
        """Migrate existing hardcoded products to database"""
        conn = None
        try:
            conn = self.get_connection()
            
            # Check if we already have data
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) as count FROM menu_categories")
                if cursor.fetchone()['count'] > 0:
                    logger.info("Menu already migrated, skipping")
                    return
            
            logger.info("🔄 Migrating existing products to database...")
            
            for category_order, (category_name, products) in enumerate(products_dict.items()):
                # Add category
                result = self.add_category(category_name, category_order)
                if not result['success']:
                    continue
                    
                category_id = result['id']
                
                for product_name, pricing_tiers in products.items():
                    # Add product
                    product_result = self.add_product(category_id, product_name)
                    if not product_result['success']:
                        continue
                        
                    product_id = product_result['id']
                    
                    # Add pricing tiers
                    for tier_order, (size, price) in enumerate(pricing_tiers):
                        self.add_pricing_tier(product_id, size, float(price), tier_order)
            
            logger.info("✅ Product migration completed")
            
        except Exception as e:
            logger.error(f"❌ Error migrating products: {e}")
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def update_product_descriptions(self, descriptions_dict: dict):
        """Update existing products with descriptions from hardcoded dictionary"""
        conn = None
        try:
            conn = self.get_connection()
            updated_count = 0
            
            with conn.cursor() as cursor:
                for product_name, description in descriptions_dict.items():
                    # Update product description where name matches
                    cursor.execute("""
                        UPDATE menu_products 
                        SET description = %s 
                        WHERE name = %s AND active = TRUE
                    """, (description, product_name))
                    
                    if cursor.rowcount > 0:
                        updated_count += cursor.rowcount
                
                conn.commit()
            
            if updated_count > 0:
                logger.info(f"✅ Updated descriptions for {updated_count} products")
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"❌ Error updating product descriptions: {e}")
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def update_product_description(self, category_name: str, product_name: str, description: str) -> dict:
        """Update a single product's description"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE menu_products 
                    SET description = %s 
                    WHERE name = %s AND category_id = (
                        SELECT id FROM menu_categories WHERE name = %s AND active = TRUE
                    ) AND active = TRUE
                """, (description, product_name, category_name))
                
                if cursor.rowcount > 0:
                    conn.commit()
                    logger.info(f"✅ Updated description for: {product_name}")
                    return {"success": True, "message": f"Description updated for '{product_name}'"}
                else:
                    return {"success": False, "error": "Product not found"}
            
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"❌ Error updating product description: {e}")
            return {"success": False, "error": str(e)}
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def toggle_product_status(self, category_name: str, product_name: str) -> dict:
        """Toggle product active status (activate/deactivate)"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # Get current status
                cursor.execute("""
                    SELECT p.active 
                    FROM menu_products p
                    JOIN menu_categories c ON p.category_id = c.id
                    WHERE c.name = %s AND p.name = %s
                """, (category_name, product_name))
                
                result = cursor.fetchone()
                if not result:
                    return {"success": False, "error": "Product not found"}
                
                current_status = result['active']
                new_status = not current_status
                
                # Toggle status
                cursor.execute("""
                    UPDATE menu_products 
                    SET active = %s 
                    WHERE name = %s AND category_id = (
                        SELECT id FROM menu_categories WHERE name = %s
                    )
                """, (new_status, product_name, category_name))
                
                conn.commit()
                action = "activated" if new_status else "deactivated"
                logger.info(f"✅ {action.capitalize()} product: {product_name}")
                return {
                    "success": True, 
                    "message": f"Product '{product_name}' {action}",
                    "new_status": "active" if new_status else "inactive"
                }
                
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"❌ Error toggling product status: {e}")
            return {"success": False, "error": str(e)}
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def get_all_products_with_status(self) -> list:
        """Get all products including inactive ones for management"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        c.name as category,
                        p.name as product,
                        p.active,
                        p.description
                    FROM menu_products p
                    JOIN menu_categories c ON p.category_id = c.id
                    WHERE c.active = TRUE
                    ORDER BY c.display_order, c.name, p.name
                """)
                
                rows = cursor.fetchall()
                products = []
                for row in rows:
                    products.append({
                        'category': row['category'],
                        'product': row['product'],
                        'active': row['active'],
                        'description': row['description']
                    })
                
                return products
                
        except Exception as e:
            logger.error(f"❌ Error getting all products: {e}")
            return []
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def get_full_menu_with_prices(self) -> list:
        """Get complete menu with all products and their prices for admin view/export"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        c.name as category,
                        p.name as product,
                        p.active,
                        p.description,
                        pr.size,
                        pr.price
                    FROM menu_categories c
                    LEFT JOIN menu_products p ON c.id = p.category_id
                    LEFT JOIN menu_pricing pr ON p.id = pr.product_id AND pr.active = TRUE
                    WHERE c.active = TRUE
                    ORDER BY c.display_order, c.name, 
                             COALESCE((SELECT MIN(price) FROM menu_pricing WHERE product_id = p.id AND active = TRUE), 999999),
                             p.name, pr.price
                """)
                
                rows = cursor.fetchall()
                products = []
                for row in rows:
                    if row['product']:
                        products.append({
                            'category': row['category'],
                            'product': row['product'],
                            'active': row['active'],
                            'description': row['description'] or '',
                            'size': row['size'] or 'N/A',
                            'price': float(row['price']) if row['price'] else 0
                        })
                
                return products
                
        except Exception as e:
            logger.error(f"❌ Error getting full menu with prices: {e}")
            return []
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def get_menu_sorted_by_price(self) -> dict:
        """Get menu with products sorted by lowest price (cheapest first)"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        c.name as category_name,
                        p.name as product_name,
                        p.description as product_description,
                        pr.size,
                        pr.price,
                        (SELECT MIN(price) FROM menu_pricing WHERE product_id = p.id AND active = TRUE) as min_price
                    FROM menu_categories c
                    LEFT JOIN menu_products p ON c.id = p.category_id AND p.active = TRUE
                    LEFT JOIN menu_pricing pr ON p.id = pr.product_id AND pr.active = TRUE
                    WHERE c.active = TRUE
                    ORDER BY c.display_order, c.name, min_price ASC NULLS LAST, p.name, pr.price
                """)
                
                rows = cursor.fetchall()
                menu = {}
                descriptions = {}
                
                for row in rows:
                    category = row['category_name']
                    product = row['product_name']
                    description = row['product_description']
                    size = row['size']
                    price = row['price']
                    
                    if category not in menu:
                        menu[category] = {}
                    
                    if product and description:
                        descriptions[product] = description
                    
                    if product and size and price:
                        if product not in menu[category]:
                            menu[category][product] = {}
                        menu[category][product][size] = float(price)
                
                return menu
                
        except Exception as e:
            logger.error(f"❌ Error getting menu sorted by price: {e}")
            return {}
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def has_claimed_freebie(self, user_id: int) -> bool:
        """Check if a user has already claimed their free edible"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id FROM freebie_claims WHERE user_id = %s
                """, (user_id,))
                result = cursor.fetchone()
                return result is not None
        except Exception as e:
            logger.error(f"❌ Error checking freebie claim: {e}")
            return False
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass
    
    def claim_freebie(self, user_id: int, username: str, product_name: str) -> dict:
        """Record a freebie claim for a user"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                # Check if already claimed
                if self.has_claimed_freebie(user_id):
                    return {"success": False, "error": "You have already claimed your free edible"}
                
                # Record the claim
                cursor.execute("""
                    INSERT INTO freebie_claims (user_id, username, claimed_product)
                    VALUES (%s, %s, %s)
                """, (user_id, username, product_name))
                
                conn.commit()
                logger.info(f"✅ Freebie claimed by user {user_id}: {product_name}")
                return {"success": True, "message": f"Freebie '{product_name}' claimed!"}
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"❌ Error claiming freebie: {e}")
            return {"success": False, "error": str(e)}
        finally:
            if conn:
                try:
                    self.put_connection(conn)
                except:
                    pass

# Global database manager instance
db_manager = None

def get_db_manager() -> DatabaseManager:
    """Get or create the global database manager instance"""
    global db_manager
    if db_manager is None:
        db_manager = DatabaseManager()
        db_manager.init_tables()
    return db_manager