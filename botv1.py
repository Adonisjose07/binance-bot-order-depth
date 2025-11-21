import os
import sys
import time
import json
import threading
import argparse
from datetime import datetime, timedelta
from dotenv import load_dotenv
from binance.client import Client
from binance.exceptions import BinanceAPIException
import pandas as pd
import numpy as np
import logging
import asyncio
import websockets
from collections import deque

# Cargar variables de entorno
load_dotenv()

class DepthConfig:
    """Clase de configuración para el análisis de profundidad"""
    
    def __init__(self):
        # Valores por defecto
        self.symbol = os.getenv('SYMBOL', 'BTCUSDT')
        self.periods = self.get_all_periods()
        self.interval_seconds = int(os.getenv('INTERVAL_SECONDS', 1))  # Cambiado a 1 segundo
        self.order_book_limit = int(os.getenv('ORDER_BOOK_LIMIT', 1000))  # Cambiado a 1000
        self.log_level = os.getenv('LOG_LEVEL', 'INFO')
        self.api_key = os.getenv('BINANCE_API_KEY', '')
        self.secret_key = os.getenv('BINANCE_SECRET_KEY', '')
        self.websocket_port = int(os.getenv('WEBSOCKET_PORT_2', 8766))
        self.host = os.getenv('HOST', '0.0.0.0')
        self.data_log_file = os.getenv('DATA_LOG_FILE', 'depth_data.jsonl')
        
    def get_all_periods(self):
        """Todos los períodos disponibles en Binance"""
        return {
            '1m': 1,
            '3m': 3,
            '5m': 5,
            '15m': 15,
            '30m': 30,
            '1h': 60,
            '2h': 120,
            '4h': 240,
            '6h': 360,
            '8h': 480,
            '12h': 720,
            '1d': 1440,
            '3d': 4320,
            '1w': 10080,
            '1M': 43200
        }
    
    def update_from_args(self, args):
        """Actualizar configuración desde argumentos de línea de comandos"""
        if args.symbol:
            self.symbol = args.symbol
        if args.interval:
            self.interval_seconds = args.interval
        if args.limit:
            self.order_book_limit = args.limit
        if args.port:
            self.websocket_port = args.port
        if args.host:
            self.host = args.host

class DataLogger:
    """Clase para guardar datos en formato JSONL"""
    
    def __init__(self, config: DepthConfig):
        self.config = config
        self.log_file = config.data_log_file
        self.setup_logging()
        
    def setup_logging(self):
        """Crear archivo de log si no existe"""
        try:
            with open(self.log_file, 'a') as f:
                pass  # Solo crear el archivo
            self.logger = logging.getLogger(__name__ + '.DataLogger')
            self.logger.info(f"Archivo de datos: {self.log_file}")
        except Exception as e:
            print(f"Error creando archivo de datos: {e}")
    
    def log_data(self, data_type, data):
        """Guardar datos en formato JSONL"""
        try:
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'type': data_type,
                'data': data
            }
            
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry, default=str) + '\n')
                
        except Exception as e:
            self.logger.error(f"Error guardando datos: {e}")

class PeriodData:
    """Clase para manejar los datos de un período específico"""
    
    def __init__(self, period_name, period_minutes, interval_seconds):
        self.period_name = period_name
        self.period_minutes = period_minutes
        self.interval_seconds = interval_seconds
        
        # CÁLCULO DE MUESTRAS CORRECTO:
        # Si el intervalo es 1s:
        # 1m = 60 muestras
        # 1h = 3600 muestras
        # 4h = 14400 muestras
        # Mantenemos toda la historia necesaria para el periodo.
        # Python moderno maneja deques de 50k-100k elementos sin problema de memoria RAM.
        self.max_samples = (period_minutes * 60) // interval_seconds
            
        self.data = deque(maxlen=self.max_samples)
        self.lock = threading.Lock()
        self.start_time = datetime.now()
        
    def add_sample(self, metrics):
        with self.lock:
            self.data.append(metrics)
            
    def get_analysis(self):
        with self.lock:
            if len(self.data) == 0:
                return None
                
            # Convertir a lista para pandas (esto puede ser pesado si son muchos datos, 
            # pero necesario para el análisis vectorial)
            data_list = list(self.data)
            df = pd.DataFrame(data_list)
            
            if len(df) < 2:
                return None
            
            # Calcular tiempo real transcurrido
            time_elapsed = (datetime.now() - self.start_time).total_seconds()
            max_possible_samples = self.max_samples
            
            # --- MEJORA DEL CÁLCULO DE RATIOS ---
            
            # 1. Ratio Actual (Instantáneo del último segundo)
            current_ratio = float(df.iloc[-1]['bid_depth_ratio'])
            
            # 2. Ratio Promedio Simple (Promedio de los ratios históricos)
            historical_ratios = df['bid_depth_ratio'].values
            avg_ratio = float(np.mean(historical_ratios))
            
            # 3. RATIO PONDERADO (NUEVO Y MÁS IMPORTANTE)
            # Sumamos todo el volumen de compra y venta visto en este periodo
            # Esto da más peso a los momentos de alto volumen y diferencia claramente las temporalidades
            period_total_bids = df['total_bid_volume'].sum()
            period_total_asks = df['total_ask_volume'].sum()
            
            weighted_ratio = 0
            if period_total_asks > 0:
                weighted_ratio = float(period_total_bids / period_total_asks)
            
            # Estadísticas
            std_ratio = float(np.std(historical_ratios))
            
            analysis = {
                'period': self.period_name,
                'samples': len(df),
                'duration_seconds': len(df) * self.interval_seconds,
                'data_coverage_pct': (len(df) / max_possible_samples) * 100,
                'timestamp': datetime.now().isoformat(),
                
                # Ratios Diferenciados
                'current_ratio': current_ratio,     # Lo que pasa AHORA (igual en todos)
                'avg_ratio': avg_ratio,             # Promedio matemático
                'weighted_ratio': weighted_ratio,   # LA CLAVE: Ratio real del volumen del periodo
                
                'max_ratio': float(np.max(historical_ratios)),
                'min_ratio': float(np.min(historical_ratios)),
                'std_ratio': std_ratio,
                
                # Volúmenes Totales del Periodo
                'period_bid_volume': float(period_total_bids),
                'period_ask_volume': float(period_total_asks),
                
                'bullish_samples': int((df['bid_depth_ratio'] > 1).sum()),
                'bearish_samples': int((df['bid_depth_ratio'] < 1).sum()),
            }
            
            # Tendencia basada en el ratio ponderado, no el promedio simple
            analysis['trend'] = self.calculate_trend(weighted_ratio)
            analysis['strength'] = self.calculate_strength(std_ratio)
            
            return analysis

    def calculate_trend(self, ratio):
        if ratio > 1.5: return "STRONG_BUY"     # Umbrales ajustados
        elif ratio > 1.1: return "BUY"
        elif ratio > 0.9: return "NEUTRAL"
        elif ratio > 0.6: return "SELL"
        else: return "STRONG_SELL"
    
    def calculate_strength(self, std_ratio):
        # Si la desviación es baja, la tendencia es constante y fuerte
        if std_ratio < 0.1: return "HIGH"
        elif std_ratio < 0.3: return "MEDIUM"
        else: return "LOW (VOLATILE)"
    
    def calculate_trend(self, avg_ratio):
        """Calcular tendencia basada en el ratio promedio del histórico"""
        if avg_ratio > 1.2:
            return "STRONG_BUY"
        elif avg_ratio > 1.05:
            return "BUY"
        elif avg_ratio > 0.95:
            return "NEUTRAL"
        elif avg_ratio > 0.8:
            return "SELL"
        else:
            return "STRONG_SELL"
    
    def calculate_strength(self, std_ratio):
        """Calcular fuerza de la tendencia basada en la desviación estándar"""
        if std_ratio < 0.05:
            return "HIGH"
        elif std_ratio < 0.1:
            return "MEDIUM"
        else:
            return "LOW"
    
    def calculate_reliability(self, actual_samples, data_coverage):
        """Calcular confiabilidad del análisis basado en cobertura de datos"""
        if data_coverage >= 80:
            return "HIGH"
        elif data_coverage >= 50:
            return "MEDIUM"
        elif data_coverage >= 20:
            return "LOW"
        else:
            return "VERY_LOW"

class DepthDataManager:
    """Gestiona el almacenamiento y cálculo de datos de profundidad"""
    
    def __init__(self, config: DepthConfig):
        self.config = config
        self.current_metrics = {}
        self.data_lock = threading.Lock()
        self.logger = logging.getLogger(__name__ + '.DataManager')
        self.start_time = datetime.now()
        self.data_logger = DataLogger(config)
        
        # Inicializar períodos con sus propias ventanas de datos REALES
        self.periods_data = {}
        for period_name, period_minutes in config.periods.items():
            self.periods_data[period_name] = PeriodData(
                period_name, 
                period_minutes, 
                config.interval_seconds
            )
        
    def add_data_point(self, metrics):
        """Agregar nuevo punto de datos a todos los períodos"""
        with self.data_lock:
            self.current_metrics = metrics.copy()
            
            # Guardar datos en JSONL
            self.data_logger.log_data('order_book', metrics)
            
            # Agregar a cada período individualmente
            for period_name, period_data in self.periods_data.items():
                period_data.add_sample(metrics)
                
            # DEBUG: Mostrar información de muestras periódicamente
            if len(self.periods_data['1m'].data) % 100 == 0:
                self.logger.info("Muestras REALES por período (ratios promedio):")
                for period_name in ['1m', '5m', '15m', '1h', '4h']:
                    if period_name in self.periods_data:
                        period_data = self.periods_data[period_name]
                        analysis = period_data.get_analysis()
                        if analysis:
                            samples = analysis['samples']
                            avg_ratio = analysis['avg_ratio']
                            trend = analysis['trend']
                            self.logger.info(f"  {period_name}: {samples} muestras, ratio_avg={avg_ratio:.4f}, {trend}")
    
    def get_period_analysis(self, period):
        """Obtener análisis para un período específico"""
        if period not in self.periods_data:
            return None
            
        analysis = self.periods_data[period].get_analysis()
        if analysis:
            analysis['symbol'] = self.config.symbol
        return analysis
    
    def get_all_analysis(self):
        """Obtener análisis para todos los períodos BASADO EN DATOS REALES"""
        analysis = {}
        total_time_elapsed = (datetime.now() - self.start_time).total_seconds()
        
        for period_name, period_data in self.periods_data.items():
            period_analysis = period_data.get_analysis()
            
            if period_analysis:
                period_analysis['symbol'] = self.config.symbol
                
                # Guardar análisis en JSONL
                self.data_logger.log_data('period_analysis', period_analysis)
                
                # Agregar información sobre la calidad de los datos
                period_minutes = self.config.periods[period_name]
                period_seconds = period_minutes * 60
                time_required = f"{period_minutes} min" if period_minutes < 60 else f"{period_minutes//60} h"
                
                period_analysis['info'] = {
                    'time_required': time_required,
                    'time_elapsed': f"{total_time_elapsed/60:.1f} min",
                    'is_historical_data': total_time_elapsed >= period_seconds
                }
                
                analysis[period_name] = period_analysis
            else:
                # Si no hay análisis, crear uno básico con datos actuales
                current_metrics = self.get_current_metrics()
                if current_metrics:
                    analysis[period_name] = self._create_basic_analysis(period_name, current_metrics, total_time_elapsed)
        
        return analysis
    
    def _create_basic_analysis(self, period_name, current_metrics, time_elapsed):
        """Crear análisis básico cuando no hay datos históricos suficientes"""
        period_minutes = self.config.periods[period_name]
        period_seconds = period_minutes * 60
        time_required = f"{period_minutes} min" if period_minutes < 60 else f"{period_minutes//60} h"
        ratio = current_metrics.get('bid_depth_ratio', 0)

        return {
            'period': period_name,
            'symbol': self.config.symbol,
            'samples': 1,
            'max_possible_samples': 1,
            'data_coverage': (time_elapsed / period_seconds) * 100 if period_seconds > 0 else 0,
            'timestamp': datetime.now().isoformat(),
            'current_ratio': current_metrics.get('bid_depth_ratio', 0),
            'avg_ratio': current_metrics.get('bid_depth_ratio', 0),
            'weighted_ratio': ratio, # Fallback
            'max_ratio': current_metrics.get('bid_depth_ratio', 0),
            'min_ratio': current_metrics.get('bid_depth_ratio', 0),
            'std_ratio': 0,
            'total_bid_volume': current_metrics.get('total_bid_volume', 0),
            'total_ask_volume': current_metrics.get('total_ask_volume', 0),
            'avg_bid_volume': current_metrics.get('total_bid_volume', 0),
            'avg_ask_volume': current_metrics.get('total_ask_volume', 0),
            'avg_spread_percentage': current_metrics.get('spread_percentage', 0),
            'current_spread': current_metrics.get('spread_percentage', 0),
            'bullish_samples': 1 if current_metrics.get('bid_depth_ratio', 0) > 1 else 0,
            'bearish_samples': 1 if current_metrics.get('bid_depth_ratio', 0) < 1 else 0,
            'neutral_samples': 1 if current_metrics.get('bid_depth_ratio', 0) == 1 else 0,
            'bullish_percentage': 100 if current_metrics.get('bid_depth_ratio', 0) > 1 else 0,
            'bearish_percentage': 100 if current_metrics.get('bid_depth_ratio', 0) < 1 else 0,
            'neutral_percentage': 100 if current_metrics.get('bid_depth_ratio', 0) == 1 else 0,
            'trend': self._calculate_trend(current_metrics.get('bid_depth_ratio', 0)),
            'strength': 'LOW',
            'reliability': 'VERY_LOW',
            'info': {
                'time_required': time_required,
                'time_elapsed': f"{time_elapsed/60:.1f} min", 
                'is_historical_data': time_elapsed >= period_seconds,
                'note': 'Datos insuficientes para análisis histórico'
            }
        }
    
    def _calculate_trend(self, ratio):
        """Calcular tendencia para análisis básico"""
        if ratio > 1.2:
            return "STRONG_BUY"
        elif ratio > 1.05:
            return "BUY"
        elif ratio > 0.95:
            return "NEUTRAL"
        elif ratio > 0.8:
            return "SELL"
        else:
            return "STRONG_SELL"
    
    def get_current_metrics(self):
        """Obtener métricas actuales"""
        with self.data_lock:
            return self.current_metrics.copy()

class CandleGenerator:
    """Genera velas OHLC con memoria acumulativa"""
    def __init__(self):
        self.current_candle = None
        self.lock = threading.Lock()

    def update(self, metrics):
        # 1. Calcular precio actual (Mid-Price)
        if metrics['bid_vwap'] > 0 and metrics['ask_vwap'] > 0:
            price = (metrics['bid_vwap'] + metrics['ask_vwap']) / 2
        else:
            price = (metrics['best_bid'] + metrics['best_ask']) / 2
        
        price = float(price)
        
        # 2. Obtener el timestamp truncado al minuto (EL "ID" DE LA VELA)
        # Ejemplo: si son las 14:05:35, esto devuelve 14:05:00
        timestamp_obj = metrics['timestamp'].replace(second=0, microsecond=0)
        candle_time = int(timestamp_obj.timestamp())

        with self.lock:
            # CASO A: Es la primera vela o cambiamos de minuto
            if self.current_candle is None or candle_time > self.current_candle['time']:
                
                # Si había una vela anterior, podríamos guardarla aquí si quisieras DB
                
                # Iniciamos nueva vela. Al principio O=H=L=C
                self.current_candle = {
                    'time': candle_time,  # Esta es la REFERENCIA
                    'open': price,
                    'high': price,
                    'low': price,
                    'close': price
                }
            
            # CASO B: Estamos en el mismo minuto (actualización)
            else:
                # El Open se mantiene fijo.
                # El Close se actualiza al precio actual.
                self.current_candle['close'] = price
                
                # El High sube si el precio actual es mayor al High histórico del minuto
                if price > self.current_candle['high']:
                    self.current_candle['high'] = price
                    
                # El Low baja si el precio actual es menor al Low histórico del minuto
                if price < self.current_candle['low']:
                    self.current_candle['low'] = price

            # Devolvemos una COPIA para no afectar la referencia interna
            return self.current_candle.copy()

    def get_current_candle(self):
        with self.lock:
            return self.current_candle.copy() if self.current_candle else None

class BinanceDepthAnalyzer:
    def __init__(self, config: DepthConfig):
        self.config = config
        self.data_manager = DepthDataManager(config)
        self.setup_logging()
        self.client = None
        self.running = False
        self.setup_binance_client()
        self.candle_generator = CandleGenerator() # Inicializar
        
    def setup_logging(self):
        """Configurar el sistema de logging"""
        logging.basicConfig(
            level=getattr(logging, self.config.log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
    def setup_binance_client(self):
        """Inicializar cliente de Binance"""
        try:
            self.client = Client(
                self.config.api_key, 
                self.config.secret_key
            )
            self.logger.info("Cliente de Binance inicializado correctamente")
        except Exception as e:
            self.logger.error(f"Error al inicializar cliente Binance: {e}")
            sys.exit(1)
    
    def get_order_book(self):
        """
        Obtener el libro de órdenes de Binance con depth 5000
        """
        try:
            depth = self.client.get_order_book(
                symbol=self.config.symbol,
                limit=self.config.order_book_limit  # 5000 niveles
            )
            return depth
        except BinanceAPIException as e:
            self.logger.error(f"Error API Binance: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error inesperado: {e}")
            return None
    
    def calculate_depth_metrics(self, order_book):
        """
        Calcular métricas de profundidad del mercado
        """
        if not order_book or 'bids' not in order_book or 'asks' not in order_book:
            return None
            
        bids = order_book['bids']
        asks = order_book['asks']
        
        # Convertir a arrays numpy
        bid_prices = np.array([float(bid[0]) for bid in bids])
        bid_volumes = np.array([float(bid[1]) for bid in bids])
        ask_prices = np.array([float(ask[0]) for ask in asks])
        ask_volumes = np.array([float(ask[1]) for ask in asks])
        
        # Calcular volúmenes totales (más preciso con 5000 niveles)
        total_bid_volume = np.sum(bid_volumes)
        total_ask_volume = np.sum(ask_volumes)
        
        # Ratio de profundidad (métrica principal)
        if total_ask_volume > 0:
            bid_depth_ratio = total_bid_volume / total_ask_volume
        else:
            bid_depth_ratio = 0
        
        # Precios promedios ponderados
        bid_vwap = np.sum(bid_prices * bid_volumes) / total_bid_volume if total_bid_volume > 0 else 0
        ask_vwap = np.sum(ask_prices * ask_volumes) / total_ask_volume if total_ask_volume > 0 else 0
        spread = ask_vwap - bid_vwap
        
        # Métricas adicionales
        best_bid = float(bids[0][0]) if bids else 0
        best_ask = float(asks[0][0]) if asks else 0
        immediate_spread = best_ask - best_bid
        
        metrics = {
            'symbol': self.config.symbol,
            'timestamp': datetime.now(),
            'total_bid_volume': total_bid_volume,
            'total_ask_volume': total_ask_volume,
            'bid_depth_ratio': bid_depth_ratio,
            'bid_vwap': bid_vwap,
            'ask_vwap': ask_vwap,
            'spread': spread,
            'spread_percentage': (spread / bid_vwap) * 100 if bid_vwap > 0 else 0,
            'best_bid': best_bid,
            'best_ask': best_ask,
            'immediate_spread': immediate_spread,
            'immediate_spread_percentage': (immediate_spread / best_bid) * 100 if best_bid > 0 else 0,
            'bid_levels': len(bids),
            'ask_levels': len(asks),
            'pressure': 'BUY' if bid_depth_ratio > 1 else 'SELL'
        }
        
        return metrics
    
    def start_data_collection(self):
        """Iniciar recolección continua de datos - ACUMULACIÓN PROGRESIVA"""
        self.running = True
        self.logger.info(f"Iniciando recolección de datos para {self.config.symbol}")
        self.logger.info(f"Intervalo: {self.config.interval_seconds} segundo(s)")
        self.logger.info(f"Depth: {self.config.order_book_limit} niveles")
        self.logger.info(f"Archivo de datos: {self.config.data_log_file}")
        self.logger.info("⚠️  Los análisis históricos se construirán progresivamente con el tiempo")
        
        sample_count = 0
        while self.running:
            try:
                # SOLO obtenemos datos ACTUALES de Binance
                order_book = self.get_order_book()
                if order_book:
                    metrics = self.calculate_depth_metrics(order_book)
                    if metrics:
                        # ---> NUEVO: Generar Velas
                        closed_candle = self.candle_generator.update(metrics)
                        
                        # Guardamos la vela actual (en formación) para enviarla en tiempo real
                        self.current_candle = self.candle_generator.get_current_candle()
                        self.last_closed_candle = closed_candle
                        # <--- FIN NUEVO
                        self.data_manager.add_data_point(metrics)
                        sample_count += 1
                        
                        # Log cada 60 muestras (cada minuto aprox)
                        if sample_count % 60 == 0:
                            elapsed_time = (datetime.now() - self.data_manager.start_time).total_seconds()
                            self.logger.info(f"Muestra #{sample_count} - Tiempo: {elapsed_time/60:.1f} min - Ratio: {metrics['bid_depth_ratio']:.4f}")
                
                time.sleep(self.config.interval_seconds)
                
            except KeyboardInterrupt:
                self.logger.info("Recolección interrumpida por el usuario")
                break
            except Exception as e:
                self.logger.error(f"Error en recolección: {e}")
                time.sleep(self.config.interval_seconds)
    
    def stop_data_collection(self):
        """Detener recolección de datos"""
        self.running = False

# WebSocketServer se mantiene igual (omitiendo por brevedad, pero con los mismos cambios)

class WebSocketServer:
    def __init__(self, analyzer: BinanceDepthAnalyzer, config: DepthConfig):
        self.analyzer = analyzer
        self.config = config
        self.connected_clients = set()
        self.logger = logging.getLogger(__name__ + '.WebSocket')
    
    async def handler(self, websocket, path):
        client_id = f"{websocket.remote_address[0]}:{websocket.remote_address[1]}"
        self.connected_clients.add(websocket)
        self.logger.info(f"Cliente conectado: {client_id}")
        
        try:
            await self.send_complete_data(websocket)
            
            async for message in websocket:
                try:
                    data = json.loads(message)
                    if data.get('type') == 'PING':
                        await websocket.send(json.dumps({'type': 'PONG', 'timestamp': datetime.now().isoformat()}))
                except json.JSONDecodeError:
                    self.logger.warning(f"Mensaje inválido de {client_id}")
                
        except websockets.exceptions.ConnectionClosed:
            self.logger.info(f"Cliente desconectado: {client_id}")
        finally:
            self.connected_clients.remove(websocket)
    
    async def send_complete_data(self, websocket):
        try:
            current_metrics = self.analyzer.data_manager.get_current_metrics()
            period_analysis = self.analyzer.data_manager.get_all_analysis()
            # Obtener vela actual en formación
            current_candle = self.analyzer.candle_generator.get_current_candle()

            complete_data = {
                'type': 'COMPLETE_DATA',
                'symbol': self.config.symbol,
                'timestamp': datetime.now().isoformat(),
                'config': {
                    'interval_seconds': self.config.interval_seconds,
                    'order_book_limit': self.config.order_book_limit,
                    'available_periods': list(self.config.periods.keys())
                },
                'current_metrics': self.serialize_metrics(current_metrics),
                'period_analysis': period_analysis,
                'system_info': {
                    'start_time': self.analyzer.data_manager.start_time.isoformat(),
                    'total_samples': sum(len(period.data) for period in self.analyzer.data_manager.periods_data.values())
                },
                'candle': current_candle, # <--- AGREGAR ESTO
            }
            # Limpiar la bandera de vela cerrada para no enviarla repetida si no es necesario
            if self.analyzer.last_closed_candle:
                self.analyzer.last_closed_candle = None

            await websocket.send(json.dumps(complete_data, default=str))
            
        except Exception as e:
            self.logger.error(f"Error enviando datos completos: {e}")
    
    def serialize_metrics(self, metrics):
        if not metrics:
            return {}
        
        serialized = metrics.copy()
        if 'timestamp' in serialized and isinstance(serialized['timestamp'], datetime):
            serialized['timestamp'] = serialized['timestamp'].isoformat()
        return serialized
    
    async def broadcast_complete_data(self):
        if not self.connected_clients:
            return
        
        try:
            current_metrics = self.analyzer.data_manager.get_current_metrics()
            period_analysis = self.analyzer.data_manager.get_all_analysis()
            # Obtener vela actual en formación
            current_candle = self.analyzer.candle_generator.get_current_candle()
            
            if not current_metrics or not period_analysis:
                return
            
            complete_data = {
                'type': 'COMPLETE_DATA',
                'symbol': self.config.symbol,
                'timestamp': datetime.now().isoformat(),
                'config': {
                    'interval_seconds': self.config.interval_seconds,
                    'order_book_limit': self.config.order_book_limit,
                    'available_periods': list(self.config.periods.keys())
                },
                'current_metrics': self.serialize_metrics(current_metrics),
                'period_analysis': period_analysis,
                'candle': current_candle, # <--- AGREGAR ESTO
            }
            # Limpiar la bandera de vela cerrada para no enviarla repetida si no es necesario
            if self.analyzer.last_closed_candle:
                self.analyzer.last_closed_candle = None
                
            message = json.dumps(complete_data, default=str)
            disconnected_clients = []
            
            for client in self.connected_clients:
                try:
                    await client.send(message)
                except websockets.exceptions.ConnectionClosed:
                    disconnected_clients.append(client)
            
            for client in disconnected_clients:
                self.connected_clients.remove(client)
                
        except Exception as e:
            self.logger.error(f"Error en broadcast: {e}")
    
    async def start_automatic_broadcast(self):
        broadcast_count = 0
        while True:
            try:
                await asyncio.sleep(self.config.interval_seconds)
                await self.broadcast_complete_data()
                broadcast_count += 1
                
                if broadcast_count % 60 == 0:
                    elapsed = (datetime.now() - self.analyzer.data_manager.start_time).total_seconds()
                    self.logger.info(f"Broadcast #{broadcast_count} - Tiempo ejecución: {elapsed/60:.1f} min")
                    
            except Exception as e:
                self.logger.error(f"Error en broadcast automático: {e}")
                await asyncio.sleep(1)
    
    async def start_server(self):
        self.logger.info(f"Iniciando servidor WebSocket en {self.config.host}:{self.config.websocket_port}")
        self.logger.info(f"Transmisión automática cada {self.config.interval_seconds} segundos")
        self.logger.info(f"Depth: {self.config.order_book_limit} niveles")
        
        asyncio.create_task(self.start_automatic_broadcast())
        
        server = await websockets.serve(
            self.handler,
            self.config.host,
            self.config.websocket_port,
            ping_interval=30,
            ping_timeout=10
        )
        
        return server

def parse_arguments():
    parser = argparse.ArgumentParser(description='Analizador de Profundidad de Binance con WebSocket')
    
    parser.add_argument('--symbol', type=str, help='Símbolo de trading (ej: BTCUSDT)')
    parser.add_argument('--interval', type=int, help='Intervalo de muestreo en segundos')
    parser.add_argument('--limit', type=int, help='Límite de niveles del order book')
    parser.add_argument('--port', type=int, help='Puerto del servidor WebSocket')
    parser.add_argument('--host', type=str, help='Host del servidor WebSocket')
    
    return parser.parse_args()

async def main():
    args = parse_arguments()
    
    config = DepthConfig()
    config.update_from_args(args)
    
    analyzer = BinanceDepthAnalyzer(config)
    ws_server = WebSocketServer(analyzer, config)
    
    try:
        data_thread = threading.Thread(target=analyzer.start_data_collection, daemon=True)
        data_thread.start()
        
        print("⏳ Iniciando recolección PROGRESIVA de datos...")
        print(f"📊 Símbolo: {config.symbol}")
        print(f"⏱️  Intervalo: {config.interval_seconds} segundo(s)")
        print(f"📈 Depth: {config.order_book_limit} niveles")
        print(f"💾 Archivo de datos: {config.data_log_file}")
        print("🔄 Los análisis históricos se construirán con el tiempo")
        time.sleep(3)
        
        server = await ws_server.start_server()
        
        print(f"\n✅ Servidor WebSocket iniciado en ws://{config.host}:{config.websocket_port}")
        print(f"📊 Todos los datos se guardan en: {config.data_log_file}")
        print("\n🛑 Presione Ctrl+C para detener el servidor...")
        
        await asyncio.Future()
        
    except KeyboardInterrupt:
        print("\n\n❌ Servidor detenido por el usuario")
    except Exception as e:
        print(f"\n\n💥 Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        analyzer.stop_data_collection()

if __name__ == "__main__":
    try:
        import binance
        import websockets
        import pandas
        import numpy
        print("✅ Todas las dependencias están disponibles")
    except ImportError as e:
        print(f"❌ Error de importación: {e}")
        print("Asegúrate de tener instaladas las dependencias:")
        print("pip install python-binance pandas numpy websockets python-dotenv")
        sys.exit(1)
    
    asyncio.run(main())