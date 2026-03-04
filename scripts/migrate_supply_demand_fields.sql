-- Migration: Add professional SSP fields to supply_tags and demand_endpoints
-- Run this on existing databases to add new columns

-- ═══════════════════════════════════════════════════════════
-- SUPPLY TAGS – new fields
-- ═══════════════════════════════════════════════════════════

ALTER TABLE supply_tags
  ADD COLUMN IF NOT EXISTS integration_type VARCHAR(16) DEFAULT 'tag' NOT NULL;

ALTER TABLE supply_tags
  ADD COLUMN IF NOT EXISTS pricing_type VARCHAR(16) DEFAULT 'floor' NOT NULL;

ALTER TABLE supply_tags
  ADD COLUMN IF NOT EXISTS revshare_pct DECIMAL(6,2) DEFAULT 80.00 NOT NULL;

ALTER TABLE supply_tags
  ADD COLUMN IF NOT EXISTS fixed_cpm DECIMAL(10,4) DEFAULT 0.0000 NOT NULL;

ALTER TABLE supply_tags
  ADD COLUMN IF NOT EXISTS sensitive BOOLEAN DEFAULT FALSE NOT NULL;


-- ═══════════════════════════════════════════════════════════
-- DEMAND ENDPOINTS – new fields
-- ═══════════════════════════════════════════════════════════

ALTER TABLE demand_endpoints
  ADD COLUMN IF NOT EXISTS integration_type VARCHAR(16) DEFAULT 'ortb' NOT NULL;

ALTER TABLE demand_endpoints
  ADD COLUMN IF NOT EXISTS ortb_version VARCHAR(16) DEFAULT '2.6' NOT NULL;

ALTER TABLE demand_endpoints
  ADD COLUMN IF NOT EXISTS auction_type INTEGER DEFAULT 1 NOT NULL;

ALTER TABLE demand_endpoints
  ADD COLUMN IF NOT EXISTS mime_types JSON;

ALTER TABLE demand_endpoints
  ADD COLUMN IF NOT EXISTS protocols JSON;

ALTER TABLE demand_endpoints
  ADD COLUMN IF NOT EXISTS demand_type VARCHAR(16) DEFAULT 'video' NOT NULL;

ALTER TABLE demand_endpoints
  ADD COLUMN IF NOT EXISTS sensitive BOOLEAN DEFAULT FALSE NOT NULL;

ALTER TABLE demand_endpoints
  ADD COLUMN IF NOT EXISTS regional_urls JSON;
