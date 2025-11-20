#!/bin/bash

# Cargar variables de entorno
source .env

echo "🚀 Iniciando Binance Depth Bot..."
echo "📊 Puerto WebSocket: ${WEBSOCKET_PORT_2}"

docker run -d \
  --name binance-bot \
  -p ${WEBSOCKET_PORT_2}:${WEBSOCKET_PORT_2} \
  --env-file .env \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/logs:/app/logs \
  binance-depth-bot

echo "✅ Bot ejecutándose en segundo plano"
echo "🌐 WebSocket disponible en: ws://localhost:${WEBSOCKET_PORT_2}"
echo "📋 Ver logs: docker logs -f binance-bot"