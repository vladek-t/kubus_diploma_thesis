import os
import psycopg2
import requests

DATABASE_URL = os.environ.get('DATABASE_URL')

def fetch_crypto_prices():
    print("Получение данных из API Coinbase...")
    
    coins = {'BTC': 'Bitcoin', 'ETH': 'Ethereum', 'SOL': 'Solana'}
    filtered_data = []
    
    for symbol, name in coins.items():
        path = f"/products/{symbol}-USD/ticker"
        url = f"https://api.exchange.coinbase.com{path}"
        
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        print(f"Монета {symbol} | Статус ответа: {response.status_code}")
        response.raise_for_status()
        
        try:
            data = response.json()
        except Exception as json_err:
            print(f"!!! Сервер вернул не JSON. Начало ответа:\n{response.text[:200]}")
            raise json_err

        # Извлекаем цену и объем по ключам Exchange API
        price = float(data['price'])
        volume = float(data['volume'])

        filtered_data.append({
            'symbol': symbol,
            'name': name,
            'price': price,
            'volume': volume  
        })
        
    return filtered_data

def upsert_to_db(data):
    if not data:
        print("Нет данных для записи.")
        return
        
    if not DATABASE_URL:
        raise ValueError("Переменная окружения DATABASE_URL не задана!")
        
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    query = """
        INSERT INTO public.crypto_rates (coin_symbol, coin_name, price_usd, volume_24h, updated_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (coin_symbol) 
        DO UPDATE SET 
            price_usd = EXCLUDED.price_usd,
            volume_24h = EXCLUDED.volume_24h,
            updated_at = NOW();
    """
    
    for row in data:
        cursor.execute(query, (row['symbol'], row['name'], row['price'], row['volume']))
        
    conn.commit()
    print(f"Успешно обновлено строк в БД: {len(data)}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    try:
        crypto_data = fetch_crypto_prices()
        upsert_to_db(crypto_data)
    except Exception as e:
        print(f"Ошибка при выполнении пайплайна: {e}")
