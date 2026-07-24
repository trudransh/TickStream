CREATE TABLE trades (
    id BIGINT AUTO_INCREMENT PRIMARY KEY, 
    event_uuid VARCHAR(36) UNIQUE,
    
    symbol VARCHAR(20),
    ts_event TIMESTAMP(3),
    ts_ingest TIMESTAMP(3),
    price DECIMAL(18, 8),
    qty DECIMAL(18, 8)
);