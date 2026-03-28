CREATE DATABASE IF NOT EXISTS insurance_db;
USE insurance_db;

CREATE TABLE customers (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    date_of_birth DATE NOT NULL,
    gender ENUM('M','F','O') NOT NULL,
    ssn_hash VARCHAR(64) NOT NULL,
    email VARCHAR(255),
    phone VARCHAR(20),
    address_line1 VARCHAR(255),
    city VARCHAR(100),
    state CHAR(2),
    zip_code VARCHAR(10),
    credit_score INT,
    occupation VARCHAR(100),
    annual_income DECIMAL(12,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE underwriters (
    underwriter_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    employee_id VARCHAR(20) NOT NULL UNIQUE,
    specialization ENUM('AUTO','HOME','LIFE','HEALTH','COMMERCIAL','GENERAL') NOT NULL DEFAULT 'GENERAL',
    experience_years INT DEFAULT 0,
    approval_limit DECIMAL(14,2) DEFAULT 0,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE adjusters (
    adjuster_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    employee_id VARCHAR(20) NOT NULL UNIQUE,
    region VARCHAR(50),
    specialization ENUM('AUTO','PROPERTY','LIABILITY','MEDICAL') NOT NULL DEFAULT 'AUTO',
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE policies (
    policy_id INT AUTO_INCREMENT PRIMARY KEY,
    policy_number VARCHAR(20) NOT NULL UNIQUE,
    customer_id INT NOT NULL,
    product_type ENUM('AUTO','HOME','LIFE','HEALTH','COMMERCIAL') NOT NULL,
    coverage_amount DECIMAL(14,2) NOT NULL,
    premium_amount DECIMAL(10,2) NOT NULL,
    deductible DECIMAL(10,2) NOT NULL DEFAULT 0,
    effective_date DATE NOT NULL,
    expiration_date DATE NOT NULL,
    status ENUM('ACTIVE','EXPIRED','CANCELLED','SUSPENDED') NOT NULL DEFAULT 'ACTIVE',
    underwriting_status ENUM('PENDING','APPROVED','DECLINED','REFERRED') NOT NULL DEFAULT 'PENDING',
    risk_score DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
) ENGINE=InnoDB;

CREATE TABLE underwriting_decisions (
    decision_id INT AUTO_INCREMENT PRIMARY KEY,
    policy_id INT NOT NULL,
    underwriter_id INT NOT NULL,
    decision ENUM('APPROVED','DECLINED','REFERRED','COUNTER_OFFER') NOT NULL,
    risk_category ENUM('LOW','MEDIUM','HIGH','VERY_HIGH') NOT NULL,
    risk_score DECIMAL(5,2),
    premium_adjustment_pct DECIMAL(5,2) DEFAULT 0,
    conditions TEXT,
    notes TEXT,
    decision_date TIMESTAMP NOT NULL,
    review_flag BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (policy_id) REFERENCES policies(policy_id),
    FOREIGN KEY (underwriter_id) REFERENCES underwriters(underwriter_id)
) ENGINE=InnoDB;

CREATE TABLE claims (
    claim_id INT AUTO_INCREMENT PRIMARY KEY,
    claim_number VARCHAR(20) NOT NULL UNIQUE,
    policy_id INT NOT NULL,
    customer_id INT NOT NULL,
    incident_date DATE NOT NULL,
    reported_date DATE NOT NULL,
    claim_type ENUM('COLLISION','THEFT','FIRE','WATER','LIABILITY','MEDICAL','PROPERTY') NOT NULL,
    description TEXT,
    estimated_amount DECIMAL(12,2),
    approved_amount DECIMAL(12,2),
    status ENUM('OPEN','UNDER_REVIEW','APPROVED','DENIED','SETTLED','CLOSED') NOT NULL DEFAULT 'OPEN',
    priority ENUM('LOW','MEDIUM','HIGH','URGENT') NOT NULL DEFAULT 'MEDIUM',
    adjuster_id INT,
    fraud_flag BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (policy_id) REFERENCES policies(policy_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (adjuster_id) REFERENCES adjusters(adjuster_id)
) ENGINE=InnoDB;

CREATE TABLE claim_payments (
    payment_id INT AUTO_INCREMENT PRIMARY KEY,
    claim_id INT NOT NULL,
    payment_date DATE NOT NULL,
    amount DECIMAL(12,2) NOT NULL,
    payment_type ENUM('PARTIAL','FINAL','SUPPLEMENT') NOT NULL DEFAULT 'FINAL',
    payment_method ENUM('CHECK','ACH','WIRE') NOT NULL DEFAULT 'ACH',
    payee_name VARCHAR(255),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (claim_id) REFERENCES claims(claim_id)
) ENGINE=InnoDB;

CREATE TABLE risk_factors (
    factor_id INT AUTO_INCREMENT PRIMARY KEY,
    policy_id INT NOT NULL,
    factor_type VARCHAR(50) NOT NULL,
    factor_value VARCHAR(255) NOT NULL,
    impact_score DECIMAL(5,2) NOT NULL DEFAULT 0,
    source VARCHAR(50),
    assessed_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (policy_id) REFERENCES policies(policy_id)
) ENGINE=InnoDB;

CREATE INDEX idx_policies_customer ON policies(customer_id);
CREATE INDEX idx_policies_status ON policies(status);
CREATE INDEX idx_policies_product ON policies(product_type);
CREATE INDEX idx_decisions_policy ON underwriting_decisions(policy_id);
CREATE INDEX idx_decisions_date ON underwriting_decisions(decision_date);
CREATE INDEX idx_claims_policy ON claims(policy_id);
CREATE INDEX idx_claims_customer ON claims(customer_id);
CREATE INDEX idx_claims_status ON claims(status);
CREATE INDEX idx_claims_type ON claims(claim_type);
CREATE INDEX idx_payments_claim ON claim_payments(claim_id);
CREATE INDEX idx_risk_factors_policy ON risk_factors(policy_id);
