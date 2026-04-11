-- ============================================================
-- RecoMart Feature Store — SQL Schema (Task 6)
-- ============================================================
-- Database: SQLite (features.db)
-- Purpose:  Store engineered features for the recommendation
--           model in a structured, queryable format.
-- ============================================================

-- ---------------------------------------------------------
-- 1. User Features
--    One row per unique customer.
--    Aggregated from transaction history.
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_features (
    customer_unique_id   TEXT PRIMARY KEY,       -- Unique user identifier
    purchase_count       INTEGER  NOT NULL,      -- Total number of purchases
    total_spending       REAL     NOT NULL,      -- Sum of all item prices (BRL)
    avg_order_value      REAL     NOT NULL,      -- Mean spending per order
    avg_rating_given     REAL,                   -- Mean review score given (1-5)
    review_count         INTEGER  NOT NULL,      -- Number of reviews submitted
    distinct_products    INTEGER  NOT NULL,      -- Number of unique products purchased
    distinct_categories  INTEGER  NOT NULL,      -- Number of unique categories purchased
    preferred_category   TEXT,                   -- Most frequently purchased category
    avg_freight          REAL     NOT NULL,      -- Mean freight cost per item
    customer_state       TEXT,                   -- Customer state code
    recency_days         INTEGER,                -- Days since last purchase
    purchase_freq_log    REAL,                   -- Log-transformed purchase frequency
    spending_log         REAL,                   -- Log-transformed total spending
    spending_normalized  REAL,                   -- Min-Max normalized total spending [0,1]
    aov_normalized       REAL,                   -- Min-Max normalized avg order value [0,1]
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------
-- 2. Item Features
--    One row per product.
--    Aggregated from sales and review data.
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS item_features (
    product_id              TEXT PRIMARY KEY,       -- Unique product identifier
    category_english        TEXT,                   -- Product category (English)
    total_sold              INTEGER  NOT NULL,      -- Total units sold
    avg_rating_received     REAL,                   -- Mean review score received
    review_count            INTEGER  NOT NULL,      -- Number of reviews received
    avg_price               REAL     NOT NULL,      -- Mean selling price (BRL)
    total_revenue           REAL     NOT NULL,      -- Total revenue generated
    distinct_buyers         INTEGER  NOT NULL,      -- Number of unique buyers
    avg_freight             REAL     NOT NULL,      -- Mean freight cost
    product_weight_g        REAL,                   -- Product weight in grams
    product_volume_cm3      REAL,                   -- Product volume (L × H × W)
    product_photos_qty      INTEGER,                -- Number of product photos
    popularity_rank         INTEGER,                -- Rank by total_sold (1 = most popular)
    price_percentile        REAL,                   -- Price percentile [0, 1]
    price_normalized        REAL,                   -- Min-Max normalized price [0,1]
    sold_normalized         REAL,                   -- Min-Max normalized total_sold [0,1]
    rating_normalized       REAL,                   -- Min-Max normalized avg_rating [0,1]
    is_cat_bed_bath_table   INTEGER DEFAULT 0,      -- One-hot: bed_bath_table
    is_cat_health_beauty    INTEGER DEFAULT 0,      -- One-hot: health_beauty
    is_cat_sports_leisure   INTEGER DEFAULT 0,      -- One-hot: sports_leisure
    is_cat_furniture_decor  INTEGER DEFAULT 0,      -- One-hot: furniture_decor
    is_cat_computers        INTEGER DEFAULT 0,      -- One-hot: computers_accessories
    is_cat_housewares       INTEGER DEFAULT 0,      -- One-hot: housewares
    is_cat_watches_gifts    INTEGER DEFAULT 0,      -- One-hot: watches_gifts
    is_cat_telephony        INTEGER DEFAULT 0,      -- One-hot: telephony
    is_cat_garden_tools     INTEGER DEFAULT 0,      -- One-hot: garden_tools
    is_cat_auto             INTEGER DEFAULT 0,      -- One-hot: auto
    is_cat_other            INTEGER DEFAULT 0,      -- One-hot: all other categories
    created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------
-- 3. Interaction Features
--    One row per user-item pair.
--    Enhanced from the base interaction matrix.
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS interaction_features (
    customer_unique_id   TEXT     NOT NULL,         -- FK → user_features
    product_id           TEXT     NOT NULL,         -- FK → item_features
    rating               REAL,                     -- Explicit rating (review score)
    purchase_count       INTEGER  NOT NULL,         -- Times this user bought this item
    implicit_signal      INTEGER  NOT NULL,         -- Binary: 1 = purchased
    rating_normalized    REAL,                      -- Min-Max normalized rating [0,1]
    user_item_affinity   REAL,                      -- Composite affinity score
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer_unique_id, product_id),
    FOREIGN KEY (customer_unique_id) REFERENCES user_features(customer_unique_id),
    FOREIGN KEY (product_id) REFERENCES item_features(product_id)
);

-- ---------------------------------------------------------
-- 4. Feature Registry (metadata about each feature)
-- ---------------------------------------------------------
CREATE TABLE IF NOT EXISTS feature_registry (
    feature_name         TEXT PRIMARY KEY,          -- Feature column name
    feature_table        TEXT     NOT NULL,         -- Which table it belongs to
    feature_type         TEXT     NOT NULL,         -- numeric, categorical, binary, text
    transformation       TEXT,                      -- Transformation applied
    description          TEXT,                      -- Human-readable description
    source_columns       TEXT,                      -- Original source columns used
    created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------
-- Indexes for query performance
-- ---------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_user_state ON user_features(customer_state);
CREATE INDEX IF NOT EXISTS idx_item_category ON item_features(category_english);
CREATE INDEX IF NOT EXISTS idx_item_popularity ON item_features(popularity_rank);
CREATE INDEX IF NOT EXISTS idx_interaction_user ON interaction_features(customer_unique_id);
CREATE INDEX IF NOT EXISTS idx_interaction_item ON interaction_features(product_id);
