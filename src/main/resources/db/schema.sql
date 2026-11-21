CREATE TABLE IF NOT EXISTS crawl_sessions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    status VARCHAR(16) NOT NULL,
    completion_reason VARCHAR(255),
    visited_count INT NOT NULL DEFAULT 0,
    matched_count INT NOT NULL DEFAULT 0,
    error_count INT NOT NULL DEFAULT 0,
    start_url VARCHAR(2048) NOT NULL,
    search_term VARCHAR(512) NOT NULL,
    strategy VARCHAR(16) NOT NULL,
    max_depth INT NOT NULL,
    page_limit INT NOT NULL,
    whole_word BOOLEAN NOT NULL,
    include_subdomains BOOLEAN NOT NULL,
    enabled_filters VARCHAR(255) NOT NULL,
    is_distributed BOOLEAN NOT NULL DEFAULT FALSE,
    server_port INT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sites (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    host VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS pages (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    session_id BIGINT NOT NULL,
    site_id BIGINT NOT NULL,
    url VARCHAR(2048) NOT NULL,
    title VARCHAR(1024),
    downloaded_at TIMESTAMP NOT NULL,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    depth INT NOT NULL,
    http_status INT NOT NULL DEFAULT 0,
    error_text VARCHAR(2048),
    has_term BOOLEAN NOT NULL DEFAULT FALSE,
    file_path VARCHAR(1024),
    CONSTRAINT fk_pages_session FOREIGN KEY (session_id) REFERENCES crawl_sessions(id) ON DELETE CASCADE,
    CONSTRAINT fk_pages_site FOREIGN KEY (site_id) REFERENCES sites(id),
    CONSTRAINT uq_pages_session_url UNIQUE (session_id, url)
);

CREATE INDEX IF NOT EXISTS idx_pages_session_id ON pages(session_id);
CREATE INDEX IF NOT EXISTS idx_pages_site_id ON pages(site_id);

CREATE TABLE IF NOT EXISTS state_snapshots (
    session_id BIGINT PRIMARY KEY,
    settings_json CLOB NOT NULL,
    queue_json CLOB NOT NULL,
    counters_json CLOB NOT NULL,
    saved_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_snapshots_session FOREIGN KEY (session_id) REFERENCES crawl_sessions(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS cache_entries (
    cache_key VARCHAR(2048) NOT NULL,
    entry_type VARCHAR(16) NOT NULL,
    site_id BIGINT,
    file_path VARCHAR(1024) NOT NULL,
    http_status INT NOT NULL,
    content_length BIGINT NOT NULL,
    final_url VARCHAR(2048),
    cached_at TIMESTAMP NOT NULL,
    PRIMARY KEY (cache_key, entry_type),
    CONSTRAINT fk_cache_site FOREIGN KEY (site_id) REFERENCES sites(id)
);

CREATE INDEX IF NOT EXISTS idx_cache_entries_cached_at ON cache_entries(cached_at);
