-- 1. Log cho bảng USERS
CREATE TABLE USERS_LOG_BEFORE (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT,
    login VARCHAR(255),
    avatar_url VARCHAR(512),
    url VARCHAR(512),
    type VARCHAR(50),
    site_admin BOOLEAN,
    created_at TIMESTAMP,
    status VARCHAR(50),
    log_timestamp TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
);

CREATE TABLE USERS_LOG_AFTER (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT,
    login VARCHAR(255),
    avatar_url VARCHAR(512),
    url VARCHAR(512),
    type VARCHAR(50),
    site_admin BOOLEAN,
    created_at TIMESTAMP,
    status VARCHAR(50),
    log_timestamp TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
);

-- 2. Log cho bảng REPOS
CREATE TABLE REPOS_LOG_BEFORE (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    repo_id BIGINT,
    name VARCHAR(255),
    full_name VARCHAR(512),
    url VARCHAR(512),
    html_url VARCHAR(512),
    description TEXT,
    is_private BOOLEAN,
    is_fork BOOLEAN,
    homepage VARCHAR(512),
    size INT,
    language VARCHAR(100),
    forks_count INT,
    stargazers_count INT,
    watchers_count INT,
    default_branch VARCHAR(100),
    created_at DATETIME,
    updated_at DATETIME,
    pushed_at DATETIME,
    status VARCHAR(50),
    log_timestamp TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
);

CREATE TABLE REPOS_LOG_AFTER (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    repo_id BIGINT,
    name VARCHAR(255),
    full_name VARCHAR(512),
    url VARCHAR(512),
    html_url VARCHAR(512),
    description TEXT,
    is_private BOOLEAN,
    is_fork BOOLEAN,
    homepage VARCHAR(512),
    size INT,
    language VARCHAR(100),
    forks_count INT,
    stargazers_count INT,
    watchers_count INT,
    default_branch VARCHAR(100),
    created_at DATETIME,
    updated_at DATETIME,
    pushed_at DATETIME,
    status VARCHAR(50),
    log_timestamp TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
);

-- 3. Log cho bảng ORGS
CREATE TABLE ORGS_LOG_BEFORE (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    org_id BIGINT,
    login VARCHAR(255),
    url VARCHAR(512),
    avatar_url VARCHAR(512),
    created_at TIMESTAMP,
    status VARCHAR(50),
    log_timestamp TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
);

CREATE TABLE ORGS_LOG_AFTER (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    org_id BIGINT,
    login VARCHAR(255),
    url VARCHAR(512),
    avatar_url VARCHAR(512),
    created_at TIMESTAMP,
    status VARCHAR(50),
    log_timestamp TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
);

-- 4. Log cho bảng EVENTS
CREATE TABLE EVENTS_LOG_BEFORE (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(50),
    event_type VARCHAR(100),
    actor_id BIGINT,
    repo_id BIGINT,
    org_id BIGINT,
    is_public BOOLEAN,
    created_at DATETIME,
    forkee_repo_id BIGINT,
    status VARCHAR(50),
    log_timestamp TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
);

CREATE TABLE EVENTS_LOG_AFTER (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(50),
    event_type VARCHAR(100),
    actor_id BIGINT,
    repo_id BIGINT,
    org_id BIGINT,
    is_public BOOLEAN,
    created_at DATETIME,
    forkee_repo_id BIGINT,
    status VARCHAR(50),
    log_timestamp TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
);

-- 5. Log cho bảng REPO_OWNERSHIP
CREATE TABLE REPO_OWNERSHIP_LOG_BEFORE (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    repo_id BIGINT,
    owner_user_id BIGINT,
    owner_org_id BIGINT,
    status VARCHAR(50),
    log_timestamp TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
);

CREATE TABLE REPO_OWNERSHIP_LOG_AFTER (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    repo_id BIGINT,
    owner_user_id BIGINT,
    owner_org_id BIGINT,
    status VARCHAR(50),
    log_timestamp TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3)
);

DELIMITER //

-- -------------------------------------------------------------------------
-- 1. TRIGGERS CHO BẢNG USERS
-- -------------------------------------------------------------------------
CREATE TRIGGER before_users_insert BEFORE INSERT ON USERS FOR EACH ROW
BEGIN
    INSERT INTO USERS_LOG_BEFORE (user_id, login, avatar_url, url, type, site_admin, created_at, status)
    VALUES (NEW.user_id, NEW.login, NEW.avatar_url, NEW.url, NEW.type, NEW.site_admin, NEW.created_at, 'INSERT');
END //

CREATE TRIGGER after_users_insert AFTER INSERT ON USERS FOR EACH ROW
BEGIN
    INSERT INTO USERS_LOG_AFTER (user_id, login, avatar_url, url, type, site_admin, created_at, status)
    VALUES (NEW.user_id, NEW.login, NEW.avatar_url, NEW.url, NEW.type, NEW.site_admin, NEW.created_at, 'INSERT');
END //

CREATE TRIGGER before_users_update BEFORE UPDATE ON USERS FOR EACH ROW
BEGIN
    INSERT INTO USERS_LOG_BEFORE (user_id, login, avatar_url, url, type, site_admin, created_at, status)
    VALUES (OLD.user_id, OLD.login, OLD.avatar_url, OLD.url, OLD.type, OLD.site_admin, OLD.created_at, 'UPDATE');
END //

CREATE TRIGGER after_users_update AFTER UPDATE ON USERS FOR EACH ROW
BEGIN
    INSERT INTO USERS_LOG_AFTER (user_id, login, avatar_url, url, type, site_admin, created_at, status)
    VALUES (NEW.user_id, NEW.login, NEW.avatar_url, NEW.url, NEW.type, NEW.site_admin, NEW.created_at, 'UPDATE');
END //

CREATE TRIGGER before_users_delete BEFORE DELETE ON USERS FOR EACH ROW
BEGIN
    INSERT INTO USERS_LOG_BEFORE (user_id, login, avatar_url, url, type, site_admin, created_at, status)
    VALUES (OLD.user_id, OLD.login, OLD.avatar_url, OLD.url, OLD.type, OLD.site_admin, OLD.created_at, 'DELETE');
END //

CREATE TRIGGER after_users_delete AFTER DELETE ON USERS FOR EACH ROW
BEGIN
    INSERT INTO USERS_LOG_AFTER (user_id, login, avatar_url, url, type, site_admin, created_at, status)
    VALUES (OLD.user_id, OLD.login, OLD.avatar_url, OLD.url, OLD.type, OLD.site_admin, OLD.created_at, 'DELETE');
END //


-- -------------------------------------------------------------------------
-- 2. TRIGGERS CHO BẢNG REPOS
-- -------------------------------------------------------------------------
CREATE TRIGGER before_repos_insert BEFORE INSERT ON REPOS FOR EACH ROW
BEGIN
    INSERT INTO REPOS_LOG_BEFORE (repo_id, name, full_name, url, html_url, description, is_private, is_fork, homepage, size, language, forks_count, stargazers_count, watchers_count, default_branch, created_at, updated_at, pushed_at, status)
    VALUES (NEW.repo_id, NEW.name, NEW.full_name, NEW.url, NEW.html_url, NEW.description, NEW.is_private, NEW.is_fork, NEW.homepage, NEW.size, NEW.language, NEW.forks_count, NEW.stargazers_count, NEW.watchers_count, NEW.default_branch, NEW.created_at, NEW.updated_at, NEW.pushed_at, 'INSERT');
END //

CREATE TRIGGER after_repos_insert AFTER INSERT ON REPOS FOR EACH ROW
BEGIN
    INSERT INTO REPOS_LOG_AFTER (repo_id, name, full_name, url, html_url, description, is_private, is_fork, homepage, size, language, forks_count, stargazers_count, watchers_count, default_branch, created_at, updated_at, pushed_at, status)
    VALUES (NEW.repo_id, NEW.name, NEW.full_name, NEW.url, NEW.html_url, NEW.description, NEW.is_private, NEW.is_fork, NEW.homepage, NEW.size, NEW.language, NEW.forks_count, NEW.stargazers_count, NEW.watchers_count, NEW.default_branch, NEW.created_at, NEW.updated_at, NEW.pushed_at, 'INSERT');
END //

CREATE TRIGGER before_repos_update BEFORE UPDATE ON REPOS FOR EACH ROW
BEGIN
    INSERT INTO REPOS_LOG_BEFORE (repo_id, name, full_name, url, html_url, description, is_private, is_fork, homepage, size, language, forks_count, stargazers_count, watchers_count, default_branch, created_at, updated_at, pushed_at, status)
    VALUES (OLD.repo_id, OLD.name, OLD.full_name, OLD.url, OLD.html_url, OLD.description, OLD.is_private, OLD.is_fork, OLD.homepage, OLD.size, OLD.language, OLD.forks_count, OLD.stargazers_count, OLD.watchers_count, OLD.default_branch, OLD.created_at, OLD.updated_at, OLD.pushed_at, 'UPDATE');
END //

CREATE TRIGGER after_repos_update AFTER UPDATE ON REPOS FOR EACH ROW
BEGIN
    INSERT INTO REPOS_LOG_AFTER (repo_id, name, full_name, url, html_url, description, is_private, is_fork, homepage, size, language, forks_count, stargazers_count, watchers_count, default_branch, created_at, updated_at, pushed_at, status)
    VALUES (NEW.repo_id, NEW.name, NEW.full_name, NEW.url, NEW.html_url, NEW.description, NEW.is_private, NEW.is_fork, NEW.homepage, NEW.size, NEW.language, NEW.forks_count, NEW.stargazers_count, NEW.watchers_count, NEW.default_branch, NEW.created_at, NEW.updated_at, NEW.pushed_at, 'UPDATE');
END //

CREATE TRIGGER before_repos_delete BEFORE DELETE ON REPOS FOR EACH ROW
BEGIN
    INSERT INTO REPOS_LOG_BEFORE (repo_id, name, full_name, url, html_url, description, is_private, is_fork, homepage, size, language, forks_count, stargazers_count, watchers_count, default_branch, created_at, updated_at, pushed_at, status)
    VALUES (OLD.repo_id, OLD.name, OLD.full_name, OLD.url, OLD.html_url, OLD.description, OLD.is_private, OLD.is_fork, OLD.homepage, OLD.size, OLD.language, OLD.forks_count, OLD.stargazers_count, OLD.watchers_count, OLD.default_branch, OLD.created_at, OLD.updated_at, OLD.pushed_at, 'DELETE');
END //

CREATE TRIGGER after_repos_delete AFTER DELETE ON REPOS FOR EACH ROW
BEGIN
    INSERT INTO REPOS_LOG_AFTER (repo_id, name, full_name, url, html_url, description, is_private, is_fork, homepage, size, language, forks_count, stargazers_count, watchers_count, default_branch, created_at, updated_at, pushed_at, status)
    VALUES (OLD.repo_id, OLD.name, OLD.full_name, OLD.url, OLD.html_url, OLD.description, OLD.is_private, OLD.is_fork, OLD.homepage, OLD.size, OLD.language, OLD.forks_count, OLD.stargazers_count, OLD.watchers_count, OLD.default_branch, OLD.created_at, OLD.updated_at, OLD.pushed_at, 'DELETE');
END //


-- -------------------------------------------------------------------------
-- 3. TRIGGERS CHO BẢNG ORGS
-- -------------------------------------------------------------------------
CREATE TRIGGER before_orgs_insert BEFORE INSERT ON ORGS FOR EACH ROW
BEGIN
    INSERT INTO ORGS_LOG_BEFORE (org_id, login, url, avatar_url, created_at, status)
    VALUES (NEW.org_id, NEW.login, NEW.url, NEW.avatar_url, NEW.created_at, 'INSERT');
END //

CREATE TRIGGER after_orgs_insert AFTER INSERT ON ORGS FOR EACH ROW
BEGIN
    INSERT INTO ORGS_LOG_AFTER (org_id, login, url, avatar_url, created_at, status)
    VALUES (NEW.org_id, NEW.login, NEW.url, NEW.avatar_url, NEW.created_at, 'INSERT');
END //

CREATE TRIGGER before_orgs_update BEFORE UPDATE ON ORGS FOR EACH ROW
BEGIN
    INSERT INTO ORGS_LOG_BEFORE (org_id, login, url, avatar_url, created_at, status)
    VALUES (OLD.org_id, OLD.login, OLD.url, OLD.avatar_url, OLD.created_at, 'UPDATE');
END //

CREATE TRIGGER after_orgs_update AFTER UPDATE ON ORGS FOR EACH ROW
BEGIN
    INSERT INTO ORGS_LOG_AFTER (org_id, login, url, avatar_url, created_at, status)
    VALUES (NEW.org_id, NEW.login, NEW.url, NEW.avatar_url, NEW.created_at, 'UPDATE');
END //

CREATE TRIGGER before_orgs_delete BEFORE DELETE ON ORGS FOR EACH ROW
BEGIN
    INSERT INTO ORGS_LOG_BEFORE (org_id, login, url, avatar_url, created_at, status)
    VALUES (OLD.org_id, OLD.login, OLD.url, OLD.avatar_url, OLD.created_at, 'DELETE');
END //

CREATE TRIGGER after_orgs_delete AFTER DELETE ON ORGS FOR EACH ROW
BEGIN
    INSERT INTO ORGS_LOG_AFTER (org_id, login, url, avatar_url, created_at, status)
    VALUES (OLD.org_id, OLD.login, OLD.url, OLD.avatar_url, OLD.created_at, 'DELETE');
END //


-- -------------------------------------------------------------------------
-- 4. TRIGGERS CHO BẢNG EVENTS
-- -------------------------------------------------------------------------
CREATE TRIGGER before_events_insert BEFORE INSERT ON EVENTS FOR EACH ROW
BEGIN
    INSERT INTO EVENTS_LOG_BEFORE (event_id, event_type, actor_id, repo_id, org_id, is_public, created_at, forkee_repo_id, status)
    VALUES (NEW.event_id, NEW.event_type, NEW.actor_id, NEW.repo_id, NEW.org_id, NEW.is_public, NEW.created_at, NEW.forkee_repo_id, 'INSERT');
END //

CREATE TRIGGER after_events_insert AFTER INSERT ON EVENTS FOR EACH ROW
BEGIN
    INSERT INTO EVENTS_LOG_AFTER (event_id, event_type, actor_id, repo_id, org_id, is_public, created_at, forkee_repo_id, status)
    VALUES (NEW.event_id, NEW.event_type, NEW.actor_id, NEW.repo_id, NEW.org_id, NEW.is_public, NEW.created_at, NEW.forkee_repo_id, 'INSERT');
END //

CREATE TRIGGER before_events_update BEFORE UPDATE ON EVENTS FOR EACH ROW
BEGIN
    INSERT INTO EVENTS_LOG_BEFORE (event_id, event_type, actor_id, repo_id, org_id, is_public, created_at, forkee_repo_id, status)
    VALUES (OLD.event_id, OLD.event_type, OLD.actor_id, OLD.repo_id, OLD.org_id, OLD.is_public, OLD.created_at, OLD.forkee_repo_id, 'UPDATE');
END //

CREATE TRIGGER after_events_update AFTER UPDATE ON EVENTS FOR EACH ROW
BEGIN
    INSERT INTO EVENTS_LOG_AFTER (event_id, event_type, actor_id, repo_id, org_id, is_public, created_at, forkee_repo_id, status)
    VALUES (NEW.event_id, NEW.event_type, NEW.actor_id, NEW.repo_id, NEW.org_id, NEW.is_public, NEW.created_at, NEW.forkee_repo_id, 'UPDATE');
END //

CREATE TRIGGER before_events_delete BEFORE DELETE ON EVENTS FOR EACH ROW
BEGIN
    INSERT INTO EVENTS_LOG_BEFORE (event_id, event_type, actor_id, repo_id, org_id, is_public, created_at, forkee_repo_id, status)
    VALUES (OLD.event_id, OLD.event_type, OLD.actor_id, OLD.repo_id, OLD.org_id, OLD.is_public, OLD.created_at, OLD.forkee_repo_id, 'DELETE');
END //

CREATE TRIGGER after_events_delete AFTER DELETE ON EVENTS FOR EACH ROW
BEGIN
    INSERT INTO EVENTS_LOG_AFTER (event_id, event_type, actor_id, repo_id, org_id, is_public, created_at, forkee_repo_id, status)
    VALUES (OLD.event_id, OLD.event_type, OLD.actor_id, OLD.repo_id, OLD.org_id, OLD.is_public, OLD.created_at, OLD.forkee_repo_id, 'DELETE');
END //


-- -------------------------------------------------------------------------
-- 5. TRIGGERS CHO BẢNG REPO_OWNERSHIP
-- -------------------------------------------------------------------------
CREATE TRIGGER before_ownership_insert BEFORE INSERT ON REPO_OWNERSHIP FOR EACH ROW
BEGIN
    INSERT INTO REPO_OWNERSHIP_LOG_BEFORE (repo_id, owner_user_id, owner_org_id, status)
    VALUES (NEW.repo_id, NEW.owner_user_id, NEW.owner_org_id, 'INSERT');
END //

CREATE TRIGGER after_ownership_insert AFTER INSERT ON REPO_OWNERSHIP FOR EACH ROW
BEGIN
    INSERT INTO REPO_OWNERSHIP_LOG_AFTER (repo_id, owner_user_id, owner_org_id, status)
    VALUES (NEW.repo_id, NEW.owner_user_id, NEW.owner_org_id, 'INSERT');
END //

CREATE TRIGGER before_ownership_update BEFORE UPDATE ON REPO_OWNERSHIP FOR EACH ROW
BEGIN
    INSERT INTO REPO_OWNERSHIP_LOG_BEFORE (repo_id, owner_user_id, owner_org_id, status)
    VALUES (OLD.repo_id, OLD.owner_user_id, OLD.owner_org_id, 'UPDATE');
END //

CREATE TRIGGER after_ownership_update AFTER UPDATE ON REPO_OWNERSHIP FOR EACH ROW
BEGIN
    INSERT INTO REPO_OWNERSHIP_LOG_AFTER (repo_id, owner_user_id, owner_org_id, status)
    VALUES (NEW.repo_id, NEW.owner_user_id, NEW.owner_org_id, 'UPDATE');
END //

CREATE TRIGGER before_ownership_delete BEFORE DELETE ON REPO_OWNERSHIP FOR EACH ROW
BEGIN
    INSERT INTO REPO_OWNERSHIP_LOG_BEFORE (repo_id, owner_user_id, owner_org_id, status)
    VALUES (OLD.repo_id, OLD.owner_user_id, OLD.owner_org_id, 'DELETE');
END //

CREATE TRIGGER after_ownership_delete AFTER DELETE ON REPO_OWNERSHIP FOR EACH ROW
BEGIN
    INSERT INTO REPO_OWNERSHIP_LOG_AFTER (repo_id, owner_user_id, owner_org_id, status)
    VALUES (OLD.repo_id, OLD.owner_user_id, OLD.owner_org_id, 'DELETE');
END //

DELIMITER ;