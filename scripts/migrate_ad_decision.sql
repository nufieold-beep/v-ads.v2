-- =========================================================================
-- LiteAds Analytics Migration – AdDecision + decision_id
-- =========================================================================
-- Run this once to add the ad_decision_log table and decision_id column
-- to ad_events.  Safe to re-run (uses IF NOT EXISTS / IF NOT EXISTS).

-- -------------------------------------------------------------------------
-- 1. Add decision_id column to ad_events
-- -------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'ad_events' AND column_name = 'decision_id'
    ) THEN
        ALTER TABLE ad_events ADD COLUMN decision_id VARCHAR(64);
        CREATE INDEX IF NOT EXISTS ix_ad_events_decision_id ON ad_events(decision_id);
        RAISE NOTICE 'Added decision_id column to ad_events';
    ELSE
        RAISE NOTICE 'decision_id column already exists on ad_events';
    END IF;
END $$;

-- -------------------------------------------------------------------------
-- 2. Create ad_decision_log table
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ad_decision_log (
    id                      SERIAL PRIMARY KEY,
    decision_id             VARCHAR(64) NOT NULL UNIQUE,
    request_id              VARCHAR(64) NOT NULL,
    imp_id                  VARCHAR(32) DEFAULT '1',
    bid_id                  VARCHAR(128) DEFAULT '',

    -- Supply context
    app_bundle              VARCHAR(255),
    app_name                VARCHAR(255),
    domain                  VARCHAR(255),
    publisher_id            VARCHAR(64),
    device_type             VARCHAR(32),
    os                      VARCHAR(64),
    geo_country             VARCHAR(3),
    geo_region              VARCHAR(64),
    ip                      VARCHAR(45),
    supply_tag_id           VARCHAR(100),

    -- Auction
    bid_floor               DECIMAL(10,4) DEFAULT 0.0000,
    bid_price               DECIMAL(10,4) DEFAULT 0.0000,
    net_price               DECIMAL(10,4) DEFAULT 0.0000,
    seat                    VARCHAR(128),
    deal_id                 VARCHAR(128),
    demand_endpoint_id      INTEGER,
    demand_endpoint_name    VARCHAR(255),

    -- Creative identification
    creative_id_resolved    VARCHAR(255),
    creative_id_source      VARCHAR(32),
    crid                    VARCHAR(255),
    adid                    VARCHAR(255),
    vast_creative_id        VARCHAR(255),
    vast_ad_id              VARCHAR(255),
    duration                INTEGER DEFAULT 0,
    width                   INTEGER DEFAULT 0,
    height                  INTEGER DEFAULT 0,

    -- Adomain
    adomain_list            JSONB,
    adomain_primary         VARCHAR(255),
    adomain_source          VARCHAR(32),
    iab_categories          JSONB,

    -- Markup / VAST
    adm_type                VARCHAR(16),
    has_media               BOOLEAN DEFAULT FALSE,
    vast_wrapper_depth      INTEGER DEFAULT 0,

    -- Timestamp
    decision_time           TIMESTAMP DEFAULT NOW() NOT NULL
);

-- Indexes
CREATE INDEX IF NOT EXISTS ix_decision_log_decision_id ON ad_decision_log(decision_id);
CREATE INDEX IF NOT EXISTS ix_decision_log_request_id ON ad_decision_log(request_id);
CREATE INDEX IF NOT EXISTS ix_decision_log_req_imp ON ad_decision_log(request_id, imp_id);
CREATE INDEX IF NOT EXISTS ix_decision_log_adomain_time ON ad_decision_log(adomain_primary, decision_time);
CREATE INDEX IF NOT EXISTS ix_decision_log_creative_time ON ad_decision_log(creative_id_resolved, decision_time);
CREATE INDEX IF NOT EXISTS ix_decision_log_app_bundle ON ad_decision_log(app_bundle);
CREATE INDEX IF NOT EXISTS ix_decision_log_geo_country ON ad_decision_log(geo_country);

DO $$ BEGIN RAISE NOTICE 'AdDecision analytics migration complete'; END $$;
