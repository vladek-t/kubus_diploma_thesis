CREATE TABLE public.crypto_rates (
    id SERIAL PRIMARY KEY,
    coin_symbol VARCHAR(10) NOT NULL,
    coin_name VARCHAR(50) NOT NULL,
    price_usd NUMERIC(16, 4) NOT NULL,
    volume_24h NUMERIC(20, 2),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Создаем уникальный индекс по символу, чтобы мы могли делать UPSERT (UPDATE или INSERT)
CREATE UNIQUE INDEX idx_coin_symbol ON public.crypto_rates(coin_symbol);


CREATE TABLE IF NOT EXISTS public.crypto_rates_history (
    id SERIAL PRIMARY KEY,
    coin_symbol VARCHAR NOT NULL,
    coin_name VARCHAR NOT NULL,
    price_usd DOUBLE PRECISION NOT NULL,
    volume_24h DOUBLE PRECISION,
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);