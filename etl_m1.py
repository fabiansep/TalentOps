

import pandas as pd
import sqlite3
import json
from datetime import datetime, timedelta
import random
import hashlib
from typing import Dict, List, Tuple
import os

# =============================================================================
# CONFIGURACIÓN
# =============================================================================

DB_PATH = "retail_warehouse.db"
DATA_DIR = "data_sources"

# =============================================================================
# 1. GENERADORES DE DATOS SIMULADOS 
# =============================================================================

def generar_datos_pos() -> pd.DataFrame:
    """
    Simula datos del sistema POS (Point of Sale).
    En producción: SELECT * FROM ventas WHERE fecha > last_load
    """
    productos_pos = ['SKU001', 'SKU002', 'SKU003', 'SKU004', 'SKU005']
    tiendas = [1, 2, 3, 4, 5]
    
    ventas = []
    base_date = datetime.now() - timedelta(days=7)
    
    for i in range(100):
        venta = {
            'pos_id': f"POS-{random.randint(10000, 99999)}",
            'timestamp_venta': (base_date + timedelta(
                days=random.randint(0, 7),
                hours=random.randint(8, 21),
                minutes=random.randint(0, 59)
            )).strftime('%Y-%m-%d %H:%M:%S'),
            'codigo_tienda': random.choice(tiendas),
            'sku_producto': random.choice(productos_pos),
            'cantidad_vendida': random.randint(1, 5),
            'precio_unit': round(random.uniform(10.0, 500.0), 2),
            'cliente_id_pos': f"CLI-{random.randint(100, 999)}" if random.random() > 0.3 else None
        }
        ventas.append(venta)
    
    return pd.DataFrame(ventas)


def generar_datos_inventario() -> List[Dict]:
    """
    Simula respuesta de API REST del sistema de inventario.
    En producción: requests.get('https://api.inventario.com/stock')
    """
    productos = [
        {'product_code': 'P001', 'sku_mapping': 'SKU001', 'name': 'Laptop Pro 15'},
        {'product_code': 'P002', 'sku_mapping': 'SKU002', 'name': 'Mouse Inalámbrico'},
        {'product_code': 'P003', 'sku_mapping': 'SKU003', 'name': 'Teclado Mecánico'},
        {'product_code': 'P004', 'sku_mapping': 'SKU004', 'name': 'Monitor 27"'},
        {'product_code': 'P005', 'sku_mapping': 'SKU005', 'name': 'Webcam HD'},
    ]
    
    stock_data = []
    for prod in productos:
        for tienda in range(1, 6):
            stock_data.append({
                'productCode': prod['product_code'],
                'skuReference': prod['sku_mapping'],
                'productName': prod['name'],
                'locationId': f"STORE-{tienda:03d}",
                'currentStock': random.randint(0, 100),
                'lastUpdated': datetime.now().isoformat()
            })
    
    return stock_data


def generar_datos_crm() -> str:
    """
    Simula archivo CSV exportado del CRM.
    En producción: Archivo en /exports/crm_clientes.csv
    """
    clientes = []
    segmentos = ['Premium', 'Regular', 'Nuevo', 'Inactivo']
    
    for i in range(100, 1000):
        cliente = {
            'customer_id': f"CLI-{i}",
            'full_name': f"Cliente {i}",
            'email': f"cliente{i}@email.com",
            'segment': random.choice(segmentos),
            'registration_date': (datetime.now() - timedelta(days=random.randint(30, 730))).strftime('%Y-%m-%d'),
            'total_purchases': random.randint(0, 50),
            'lifetime_value': round(random.uniform(100, 10000), 2)
        }
        clientes.append(cliente)
    
    # Agregar algunos duplicados para simular datos sucios
    for _ in range(10):
        dup = clientes[random.randint(0, len(clientes)-1)].copy()
        dup['email'] = dup['email'].upper()  # Variación en el duplicado
        clientes.append(dup)
    
    df = pd.DataFrame(clientes)
    csv_content = df.to_csv(index=False)
    return csv_content


def generar_logs_web() -> List[Dict]:
    """
    Simula logs del servidor web (comportamiento online).
    En producción: Archivos JSON en /var/log/web/
    """
    eventos = ['page_view', 'add_to_cart', 'purchase', 'search', 'product_view']
    
    logs = []
    base_date = datetime.now() - timedelta(days=1)
    
    for _ in range(200):
        log = {
            'event_id': hashlib.md5(str(random.random()).encode()).hexdigest()[:16],
            'timestamp': (base_date + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )).isoformat(),
            'event_type': random.choice(eventos),
            'user_id': f"CLI-{random.randint(100, 999)}" if random.random() > 0.4 else None,
            'session_id': hashlib.md5(str(random.random()).encode()).hexdigest()[:12],
            'product_sku': random.choice(['SKU001', 'SKU002', 'SKU003', 'SKU004', 'SKU005']) if random.random() > 0.3 else None,
            'page_url': f"/producto/{random.randint(1, 100)}",
            'device': random.choice(['mobile', 'desktop', 'tablet']),
            'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge'])
        }
        logs.append(log)
    
    return logs


# =============================================================================
# 2. EXTRACTORES (Extract)
# =============================================================================

class DataExtractor:
    """Clase para extraer datos de múltiples fuentes."""
    
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
    
    def extract_pos(self) -> pd.DataFrame:
        """Extrae datos del POS (simula conexión SQL)."""
        print(" Extrayendo datos del POS...")
        df = generar_datos_pos()
        df.to_csv(f"{self.data_dir}/pos_raw.csv", index=False)
        print(f"    {len(df)} registros extraídos del POS")
        return df
    
    def extract_inventory(self) -> pd.DataFrame:
        """Extrae datos del inventario (simula llamada API REST)."""
        print(" Extrayendo datos del inventario (API)...")
        data = generar_datos_inventario()
        
        # Guardar respuesta JSON (como lo haría en producción)
        with open(f"{self.data_dir}/inventory_api_response.json", 'w') as f:
            json.dump(data, f, indent=2)
        
        df = pd.DataFrame(data)
        print(f"  ✓ {len(df)} registros extraídos del inventario")
        return df
    
    def extract_crm(self) -> pd.DataFrame:
        """Extrae datos del CRM (archivo CSV)."""
        print(" Extrayendo datos del CRM (CSV)...")
        csv_content = generar_datos_crm()
        
        # Guardar CSV (simula archivo exportado)
        with open(f"{self.data_dir}/crm_export.csv", 'w') as f:
            f.write(csv_content)
        
        df = pd.read_csv(f"{self.data_dir}/crm_export.csv")
        print(f"  {len(df)} registros extraídos del CRM")
        return df
    
    def extract_web_logs(self) -> pd.DataFrame:
        """Extrae logs del servidor web (JSON)."""
        print(" Extrayendo logs web (JSON)...")
        logs = generar_logs_web()
        
        # Guardar logs JSON
        with open(f"{self.data_dir}/web_logs.json", 'w') as f:
            json.dump(logs, f, indent=2)
        
        df = pd.DataFrame(logs)
        print(f"    {len(df)} eventos extraídos de logs web")
        return df


# =============================================================================
# 3. TRANSFORMADORES (Transform)
# =============================================================================

class DataTransformer:
    """Clase para transformar y limpiar datos."""
    
    def __init__(self):
        self.product_mapping = {}  # Cache para mapeo de productos
        self.customer_mapping = {}  # Cache para información de clientes
    
    def transform_pos(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transformaciones para datos del POS:
        - Convertir timestamps a fecha
        - Calcular total de venta
        - Normalizar IDs
        """
        print(" Transformando datos del POS...")
        
        df_transformed = df.copy()
        
        # 1. Convertir timestamp a fecha
        df_transformed['fecha_venta'] = pd.to_datetime(
            df_transformed['timestamp_venta']
        ).dt.date
        
        # 2. Calcular total de venta
        df_transformed['total_venta'] = (
            df_transformed['cantidad_vendida'] * df_transformed['precio_unit']
        ).round(2)
        
        # 3. Normalizar ID de tienda
        df_transformed['id_tienda'] = df_transformed['codigo_tienda']
        
        # 4. Crear ID único de venta (hash para deduplicación)
        df_transformed['venta_hash'] = df_transformed.apply(
            lambda x: hashlib.md5(
                f"{x['pos_id']}{x['timestamp_venta']}".encode()
            ).hexdigest()[:16],
            axis=1
        )
        
        # 5. Agregar canal de venta
        df_transformed['canal_venta'] = 'tienda'
        
        # Renombrar columnas al esquema destino
        df_transformed = df_transformed.rename(columns={
            'sku_producto': 'id_producto',
            'cantidad_vendida': 'cantidad',
            'precio_unit': 'precio_unitario',
            'cliente_id_pos': 'id_cliente'
        })
        
        print(f"   ✓ Transformación POS completada")
        return df_transformed
    
    def transform_crm(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transformaciones para datos del CRM:
        - Eliminar duplicados
        - Normalizar campos
        - Crear diccionario de segmentos
        """
        print(" Transformando datos del CRM...")
        
        df_transformed = df.copy()
        
        # 1. Normalizar email (lowercase) para detectar duplicados
        df_transformed['email_normalized'] = df_transformed['email'].str.lower()
        
        # 2. Eliminar duplicados manteniendo el más reciente
        df_transformed = df_transformed.sort_values('registration_date', ascending=False)
        df_transformed = df_transformed.drop_duplicates(
            subset=['email_normalized'], 
            keep='first'
        )
        
        # 3. Crear mapeo de cliente a segmento (para enriquecimiento)
        self.customer_mapping = dict(zip(
            df_transformed['customer_id'],
            df_transformed['segment']
        ))
        
        print(f"    {len(df_transformed)} clientes únicos después de deduplicación")
        print(f"    Mapeo de segmentos creado: {len(self.customer_mapping)} clientes")
        return df_transformed
    
    def transform_inventory(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transformaciones para datos del inventario:
        - Normalizar códigos de producto
        - Extraer ID numérico de tienda
        """
        print(" Transformando datos del inventario...")
        
        df_transformed = df.copy()
        
        # 1. Crear mapeo SKU -> código interno
        self.product_mapping = dict(zip(
            df_transformed['skuReference'].unique(),
            df_transformed['productCode'].unique()
        ))
        
        # 2. Extraer ID numérico de tienda (STORE-001 -> 1)
        df_transformed['id_tienda'] = df_transformed['locationId'].str.extract(r'(\d+)').astype(int)
        
        # 3. Convertir fecha de actualización
        df_transformed['fecha_actualizacion'] = pd.to_datetime(
            df_transformed['lastUpdated']
        ).dt.date
        
        print(f"    Mapeo de productos creado: {self.product_mapping}")
        return df_transformed
    
    def transform_web_logs(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transformaciones para logs web:
        - Filtrar solo eventos de compra
        - Normalizar timestamps
        - Preparar para integración
        """
        print(" Transformando logs web...")
        
        df_transformed = df.copy()
        
        # 1. Convertir timestamp
        df_transformed['fecha_evento'] = pd.to_datetime(
            df_transformed['timestamp']
        ).dt.date
        
        # 2. Filtrar solo compras para integrar con ventas
        df_purchases = df_transformed[df_transformed['event_type'] == 'purchase'].copy()
        df_purchases['canal_venta'] = 'online'
        
        print(f"    {len(df_purchases)} compras online identificadas")
        return df_purchases
    
    def enrich_with_customer_segment(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enriquece datos de ventas con segmento del cliente."""
        print(" Enriqueciendo con segmentos de cliente...")
        
        df['segmento_cliente'] = df['id_cliente'].map(self.customer_mapping)
        df['segmento_cliente'] = df['segmento_cliente'].fillna('Desconocido')
        
        segment_counts = df['segmento_cliente'].value_counts()
        print(f"    Distribución de segmentos:\n{segment_counts.to_string()}")
        return df


# =============================================================================
# 4. CARGADORES (Load)
# =============================================================================

class DataLoader:
    """Clase para cargar datos al data warehouse."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_database()
    
    def _init_database(self):
        """Inicializa el esquema del data warehouse."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Tabla de hechos: Ventas consolidadas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ventas_consolidadas (
                id_venta INTEGER PRIMARY KEY AUTOINCREMENT,
                venta_hash VARCHAR(16) UNIQUE,
                fecha_venta DATE,
                id_tienda INTEGER,
                id_cliente VARCHAR(20),
                id_producto VARCHAR(20),
                cantidad INTEGER,
                precio_unitario DECIMAL(10,2),
                total_venta DECIMAL(10,2),
                canal_venta VARCHAR(20),
                segmento_cliente VARCHAR(20),
                fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Dimensión: Clientes
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_clientes (
                id_cliente VARCHAR(20) PRIMARY KEY,
                nombre VARCHAR(100),
                email VARCHAR(100),
                segmento VARCHAR(20),
                fecha_registro DATE,
                total_compras INTEGER,
                valor_lifetime DECIMAL(10,2),
                fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Dimensión: Productos
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_productos (
                sku VARCHAR(20) PRIMARY KEY,
                codigo_interno VARCHAR(20),
                nombre_producto VARCHAR(100),
                fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Dimensión: Inventario
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dim_inventario (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku VARCHAR(20),
                id_tienda INTEGER,
                stock_actual INTEGER,
                fecha_actualizacion DATE,
                UNIQUE(sku, id_tienda)
            )
        """)
        
        # Tabla de control de cargas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS etl_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fuente VARCHAR(50),
                tipo_carga VARCHAR(20),
                registros_procesados INTEGER,
                registros_insertados INTEGER,
                registros_duplicados INTEGER,
                fecha_ejecucion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                estado VARCHAR(20),
                mensaje TEXT
            )
        """)
        
        conn.commit()
        conn.close()
        print(" Base de datos inicializada")
    
    def load_sales(self, df: pd.DataFrame, source: str) -> Dict:
        """
        Carga ventas con estrategia incremental (upsert).
        Detecta duplicados usando venta_hash.
        """
        print(f" Cargando ventas desde {source}...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Preparar datos para inserción
        columns = [
            'venta_hash', 'fecha_venta', 'id_tienda', 'id_cliente',
            'id_producto', 'cantidad', 'precio_unitario', 'total_venta',
            'canal_venta', 'segmento_cliente'
        ]
        
        df_to_load = df[columns].copy()
        df_to_load['fecha_venta'] = df_to_load['fecha_venta'].astype(str)
        
        # Contar duplicados
        existing_hashes = pd.read_sql(
            "SELECT venta_hash FROM ventas_consolidadas", conn
        )['venta_hash'].tolist()
        
        duplicates = df_to_load[df_to_load['venta_hash'].isin(existing_hashes)]
        new_records = df_to_load[~df_to_load['venta_hash'].isin(existing_hashes)]
        
        # Insertar solo nuevos registros
        if len(new_records) > 0:
            new_records.to_sql(
                'ventas_consolidadas',
                conn,
                if_exists='append',
                index=False
            )
        
        conn.close()
        
        stats = {
            'procesados': len(df_to_load),
            'insertados': len(new_records),
            'duplicados': len(duplicates)
        }
        
        print(f"    Procesados: {stats['procesados']}")
        print(f"    Insertados: {stats['insertados']}")
        print(f"    Duplicados ignorados: {stats['duplicados']}")
        
        self._log_execution(source, 'incremental', stats)
        return stats
    
    def load_customers(self, df: pd.DataFrame) -> Dict:
        """Carga dimensión de clientes (SCD Tipo 1 - sobrescribir)."""
        print(" Cargando dimensión de clientes...")
        
        conn = sqlite3.connect(self.db_path)
        
        df_to_load = df[['customer_id', 'full_name', 'email', 'segment',
                         'registration_date', 'total_purchases', 'lifetime_value']].copy()
        df_to_load.columns = ['id_cliente', 'nombre', 'email', 'segmento',
                              'fecha_registro', 'total_compras', 'valor_lifetime']
        
        # Estrategia: REPLACE (SCD Tipo 1)
        df_to_load.to_sql('dim_clientes', conn, if_exists='replace', index=False)
        
        conn.close()
        
        stats = {'cargados': len(df_to_load)}
        print(f"    {stats['cargados']} clientes cargados")
        
        self._log_execution('CRM', 'full_refresh', stats)
        return stats
    
    def load_inventory(self, df: pd.DataFrame) -> Dict:
        """Carga inventario (actualización completa diaria)."""
        print(" Cargando inventario...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Cargar productos únicos a dimensión
        productos = df[['skuReference', 'productCode', 'productName']].drop_duplicates()
        productos.columns = ['sku', 'codigo_interno', 'nombre_producto']
        productos.to_sql('dim_productos', conn, if_exists='replace', index=False)
        
        # Cargar stock actual
        stock = df[['skuReference', 'id_tienda', 'currentStock', 'fecha_actualizacion']].copy()
        stock.columns = ['sku', 'id_tienda', 'stock_actual', 'fecha_actualizacion']
        stock['fecha_actualizacion'] = stock['fecha_actualizacion'].astype(str)
        stock.to_sql('dim_inventario', conn, if_exists='replace', index=False)
        
        conn.close()
        
        stats = {
            'productos': len(productos),
            'registros_stock': len(stock)
        }
        print(f"   {stats['productos']} productos, {stats['registros_stock']} registros de stock")
        
        self._log_execution('Inventario', 'full_refresh', stats)
        return stats
    
    def _log_execution(self, source: str, load_type: str, stats: Dict):
        """Registra la ejecución del ETL."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO etl_log (fuente, tipo_carga, registros_procesados, 
                                 registros_insertados, registros_duplicados, estado)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            source,
            load_type,
            stats.get('procesados', stats.get('cargados', 0)),
            stats.get('insertados', stats.get('cargados', 0)),
            stats.get('duplicados', 0),
            'SUCCESS'
        ))
        
        conn.commit()
        conn.close()


# =============================================================================
# 5. ORQUESTADOR DEL PIPELINE
# =============================================================================

class RetailETLPipeline:
    """Orquestador principal del pipeline ETL."""
    
    def __init__(self, db_path: str = DB_PATH, data_dir: str = DATA_DIR):
        self.extractor = DataExtractor(data_dir)
        self.transformer = DataTransformer()
        self.loader = DataLoader(db_path)
        self.db_path = db_path
    
    def run_full_pipeline(self):
        """Ejecuta el pipeline ETL completo."""
        print("\n" + "="*60)
        print(" INICIANDO PIPELINE ETL RETAIL")
        print("="*60 + "\n")
        
        start_time = datetime.now()
        
        # ----- EXTRACT -----
        print("\n FASE 1: EXTRACCIÓN")
        print("-" * 40)
        pos_raw = self.extractor.extract_pos()
        inventory_raw = self.extractor.extract_inventory()
        crm_raw = self.extractor.extract_crm()
        web_raw = self.extractor.extract_web_logs()
        
        # ----- TRANSFORM -----
        print("\n FASE 2: TRANSFORMACIÓN")
        print("-" * 40)
        
        # Primero transformar CRM para tener mapeo de segmentos
        crm_clean = self.transformer.transform_crm(crm_raw)
        
        # Transformar inventario para tener mapeo de productos
        inventory_clean = self.transformer.transform_inventory(inventory_raw)
        
        # Transformar ventas POS
        pos_transformed = self.transformer.transform_pos(pos_raw)
        pos_enriched = self.transformer.enrich_with_customer_segment(pos_transformed)
        
        # Transformar logs web (solo compras)
        web_purchases = self.transformer.transform_web_logs(web_raw)
        
        # ----- LOAD -----
        print("\n FASE 3: CARGA")
        print("-" * 40)
        self.loader.load_customers(crm_clean)
        self.loader.load_inventory(inventory_clean)
        self.loader.load_sales(pos_enriched, 'POS')
        
        # ----- RESUMEN -----
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("\n" + "="*60)
        print(" PIPELINE COMPLETADO")
        print("="*60)
        print(f"  Duración total: {duration:.2f} segundos")
        
        self._print_warehouse_summary()
    
    def _print_warehouse_summary(self):
        """Muestra resumen del data warehouse."""
        conn = sqlite3.connect(self.db_path)
        
        print("\n RESUMEN DEL DATA WAREHOUSE:")
        print("-" * 40)
        
        # Conteo de registros
        tables = ['ventas_consolidadas', 'dim_clientes', 'dim_productos', 'dim_inventario']
        for table in tables:
            count = pd.read_sql(f"SELECT COUNT(*) as n FROM {table}", conn)['n'][0]
            print(f"   {table}: {count} registros")
        
        # Ventas por canal
        print("\n VENTAS POR CANAL:")
        ventas_canal = pd.read_sql("""
            SELECT canal_venta, 
                   COUNT(*) as num_ventas,
                   ROUND(SUM(total_venta), 2) as total
            FROM ventas_consolidadas
            GROUP BY canal_venta
        """, conn)
        print(ventas_canal.to_string(index=False))
        
        # Ventas por segmento
        print("\n VENTAS POR SEGMENTO DE CLIENTE:")
        ventas_segmento = pd.read_sql("""
            SELECT segmento_cliente,
                   COUNT(*) as num_ventas,
                   ROUND(AVG(total_venta), 2) as ticket_promedio
            FROM ventas_consolidadas
            GROUP BY segmento_cliente
            ORDER BY num_ventas DESC
        """, conn)
        print(ventas_segmento.to_string(index=False))
        
        # Log de ejecuciones
        print("\n ÚLTIMAS EJECUCIONES ETL:")
        log = pd.read_sql("""
            SELECT fuente, tipo_carga, registros_insertados, estado,
                   datetime(fecha_ejecucion) as fecha
            FROM etl_log
            ORDER BY fecha_ejecucion DESC
            LIMIT 5
        """, conn)
        print(log.to_string(index=False))
        
        conn.close()


# =============================================================================
# 6. EJECUCIÓN PRINCIPAL
# =============================================================================

if __name__ == "__main__":
    pipeline = RetailETLPipeline()
    pipeline.run_full_pipeline()
