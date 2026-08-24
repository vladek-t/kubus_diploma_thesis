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
