-- Schema and landing Volume for the CardPulse use case.
CREATE SCHEMA IF NOT EXISTS workspace.cardpulse
COMMENT 'CardPulse credit card analytics, medallion layers on Iceberg';

CREATE VOLUME IF NOT EXISTS workspace.cardpulse.landing
COMMENT 'Daily card transaction CSV files uploaded for bronze loading';
