#!/bin/bash
echo "🔨 Construyendo imagen Docker..."
docker build -t binance-depth-bot .

echo "✅ Imagen construida: binance-depth-bot"
echo "📝 Para ejecutar: docker run -p 8766:8766 --env-file .env binance-depth-bot"