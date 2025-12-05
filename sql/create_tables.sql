-- Eliminar tablas si existen
DROP TABLE IF EXISTS fact_transactions;
DROP TABLE IF EXISTS dim_payment;
DROP TABLE IF EXISTS dim_merchant;
DROP TABLE IF EXISTS dim_user;


-- =========================================================
-- Esquema Estrella (Star Schema), modelo más simple y eficiente para consultas analíticas sobre transacciones
-- =========================================================

-- =========================================================
-- 1. Tablas de Dimensiones
-- =========================================================

-- Dimensión Usuario (Solo user_id por ahora, se completaría con la tabla users.csv)
CREATE TABLE dim_user (
    user_key SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL UNIQUE
);

-- Dimensión Comercio (Solo merchant_id por ahora)
CREATE TABLE dim_merchant (
    merchant_key SERIAL PRIMARY KEY,
    merchant_id INTEGER NOT NULL UNIQUE
);

-- Dimensión Pago
CREATE TABLE dim_payment (
    payment_key SERIAL PRIMARY KEY,
    payment_method VARCHAR(50) NOT NULL,
    payment_provider VARCHAR(50) NOT NULL,
    UNIQUE (payment_method, payment_provider)
);

-- =========================================================
-- 2. Tabla de Hechos (Fact Table)
-- =========================================================


-- fact_transactions es el centro de tu esquema. Contiene las métricas y las claves que enlazan con las Dimensiones.
CREATE TABLE fact_transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Claves Foráneas (Foreign Keys)
    user_key INTEGER REFERENCES dim_user(user_key),
    merchant_key INTEGER REFERENCES dim_merchant(merchant_key),
    payment_key INTEGER REFERENCES dim_payment(payment_key),
    
    -- Hechos (Métricas)
    amount NUMERIC(10, 2) NOT NULL,
    currency VARCHAR(10) NOT NULL,
    status VARCHAR(50) NOT NULL,
    
    -- Datos de riesgo y fees
    country VARCHAR(10),
    response_code VARCHAR(10),
    transaction_fee NUMERIC(10, 2),
    net_amount NUMERIC(10, 2),
    attempt_number INTEGER,
    
    -- Indicador de fraude (ya no es la bandera 'is_suspicious', sino el dato persistido)
    is_suspicious BOOLEAN DEFAULT FALSE 
);