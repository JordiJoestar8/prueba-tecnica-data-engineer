"""
Main ETL Pipeline for Transaction Processing

This script runs continuously, generating fake transactions every minute
and processing them through a data pipeline.

TODO: Complete the following functions:
1. clean_data() - Clean and validate the raw transaction data
2. detect_suspicious_transactions() - Identify potentially fraudulent transactions
"""

import time
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from scripts.generate_transactions import generate_transactions


# Configuration
TRANSACTIONS_FOLDER = Path("./transactions")
PROCESSED_FOLDER = Path("./processed")
SUSPICIOUS_FOLDER = Path("./suspicious")
INTERVAL_SECONDS = 60  # Generate transactions every 1 minute
TRANSACTIONS_PER_BATCH = 100  # Number of transactions to generate each time


def setup_folders():
    """Create necessary folders if they don't exist"""
    TRANSACTIONS_FOLDER.mkdir(exist_ok=True)
    PROCESSED_FOLDER.mkdir(exist_ok=True)
    SUSPICIOUS_FOLDER.mkdir(exist_ok=True)
    print(f"Folders initialized:")
    print(f"  - Data Lake: {TRANSACTIONS_FOLDER}")
    print(f"  - Processed: {PROCESSED_FOLDER}")
    print(f"  - Suspicious: {SUSPICIOUS_FOLDER}")


def generate_batch():
    """Generate a batch of fake transactions and save to data lake"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = TRANSACTIONS_FOLDER / f"transactions_{timestamp}.csv"

    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Generating {TRANSACTIONS_PER_BATCH} transactions...")
    df = generate_transactions(TRANSACTIONS_PER_BATCH)
    df.to_csv(filename, index=False)
    print(f"Saved to: {filename}")

    return filename


def clean_data(df):
    """
    TODO: Implement data cleaning logic

    Clean and validate the transaction data. Consider:
    - Handling missing values
    - Removing duplicates
    - Data type validation
    - Standardizing formats
    - Handling outliers

    Args:
        df (pd.DataFrame): Raw transaction data

    Returns:
        pd.DataFrame: Cleaned transaction data
    """
    
#------------------------INICIO-LOGICA------------------------#
    df_clean = df.copy()
    rows_inicio = len(df_clean)
    
    # 1. Eliminar duplicados 
    df_clean.drop_duplicates(inplace=True)
    
    # 2. Manejar valores nulos/faltantes 
    
    # - Eliminar filas donde falten datos CRÍTICOS (ej. amount, transaction_id)
    df_clean.dropna(subset=['amount', 'transaction_id', 'user_id', 'merchant_id'], inplace=True)

    # - Rellenar valores nulos en columnas no críticas o de texto
    df_clean['country'] = df_clean['country'].fillna('ZZ') # 'ZZ' para código desconocido
    df_clean['status'] = df_clean['status'].fillna('UNKNOWN')

    # 3. Validar y convertir tipos de datos 
    
    # - Asegurar que 'amount' sea numérico. Forzamos a NaN los errores y los eliminamos.
    df_clean['amount'] = pd.to_numeric(df_clean['amount'], errors='coerce')
    df_clean.dropna(subset=['amount'], inplace=True)
    df_clean['amount'] = df_clean['amount'].astype(float)
    
    # - Convertir fechas a datetime
    df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'], errors='coerce', utc=True)
    df_clean.dropna(subset=['timestamp'], inplace=True)
    
    # 4. Estandarizar formatos 
    
    # - Estandarizar códigos de país a mayúsculas
    df_clean['country'] = df_clean['country'].str.upper()
    
    rows_final = len(df_clean)
    print(f"   - Limpieza finalizada. Filas eliminadas: {rows_inicio - rows_final}")
    return df_clean
#------------------------FIN-LOGICA------------------------#

    raise NotImplementedError("clean_data() function needs to be implemented")


def detect_suspicious_transactions(df):
    """
    TODO: Implement fraud detection logic

    Identify suspicious transactions based on various criteria. Consider:
    - Unusually high amounts
    - Multiple failed attempts
    - High-risk countries or merchants
    - Unusual transaction patterns
    - Time-based anomalies
    - Multiple transactions in short time

    Args:
        df (pd.DataFrame): Cleaned transaction data

    Returns:
        tuple: (normal_df, suspicious_df) - DataFrames split by suspicion status
    """

#------------------------INICIO-LOGICA------------------------#
    #Implementa la lógica de detección de fraude y separa los DataFrames.
    
    df['is_suspicious'] = False # Creamos una flag inicialmente con el valor de False

    # --- Criterios de Detección de Fraude ---
    
    # 1. Montos Inusualmente Altos (Outliers) 
    # Se marca cualquier transacción que esté por encima de 2 desviaciones estándar (2*std)
    # Es un indicador estadístico de que el valor es una rareza.
    MEAN_AMOUNT = df['amount'].mean()
    STD_AMOUNT = df['amount'].std()
    HIGH_AMOUNT = MEAN_AMOUNT + 2 * STD_AMOUNT  #Calculo de monto inusualmente altos
    
    # localizamos las columnas con valor mayor al HIGH_AMOUNT y cambiamos el valor de 'is_suspicious' a Tru
    df.loc[df['amount'] > HIGH_AMOUNT, 'is_suspicious'] = True
    print(f"   - Regla 1 (Monto Alto > {HIGH_AMOUNT:.2f}): {df['is_suspicious'].sum()} marcadas.")

    # 2. Transacciones Múltiples Fallidas (Patrón anómalo) 
    # Se simula el conteo de fallos por usuario/minuto.
    # Si un usuario tiene más de 3 transacciones fallidas ('declined')
    # en el mismo lote (que representa 1 minuto de ingesta), se marca como sospechoso.
    
    failed_counts = df[df['status'] == 'declined'].groupby('user_id')['transaction_id'].count()
    users_with_many_fails = failed_counts[failed_counts >= 3].index
    
    df.loc[(df['user_id'].isin(users_with_many_fails)), 'is_suspicious'] = True
    print(f"   - Regla 2 (Múltiples Fallidos): {len(users_with_many_fails)} usuarios flaggeados.")

    # 3. Transacciones Internacionales de Alto Riesgo 
    # Se marca cualquier transacción donde el país no es el país base ('PE' asumido) Y el monto es alto (ej. > 5000)
    BASE_COUNTRY = 'PE' 
    INTERNATIONAL_HIGH_AMOUNT = 1000     #Colocando un valor por defecto de 1000
    
    df.loc[(df['country'] != BASE_COUNTRY) & (df['amount'] > INTERNATIONAL_HIGH_AMOUNT), 'is_suspicious'] = True
    print(f"   - Regla 3 (Internacional de Alto Riesgo): {df['is_suspicious'].sum()} marcadas.")
    
    # --- Separar DataFrames ---
    
    df_suspicious = df[df['is_suspicious'] == True].copy()
    df_normal = df[df['is_suspicious'] == False].copy()
    
    # La columna 'is_suspicious' ya no es necesaria en los resultados
    df_suspicious.drop(columns=['is_suspicious'], inplace=True)
    df_normal.drop(columns=['is_suspicious'], inplace=True)

    return df_normal, df_suspicious
#------------------------FIN-LOGICA------------------------#

    raise NotImplementedError("detect_suspicious_transactions() function needs to be implemented")


def process_batch(raw_file):
    """
    Process a batch of transactions through the ETL pipeline

    Args:
        raw_file (Path): Path to the raw transaction CSV file
    """
    try:
        # Read raw data from data lake
        print(f"Reading data from: {raw_file}")
        df_raw = pd.read_csv(raw_file)
        print(f"Loaded {len(df_raw)} transactions")

        # Step 1: Clean the data
        print("Cleaning data...")
        df_clean = clean_data(df_raw)
        print(f"Cleaned {len(df_clean)} transactions")

        # Step 2: Detect suspicious transactions
        print("Detecting suspicious transactions...")
        df_normal, df_suspicious = detect_suspicious_transactions(df_clean)
        print(f"Found {len(df_suspicious)} suspicious transactions")
        print(f"Found {len(df_normal)} normal transactions")

        # Save processed results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if len(df_normal) > 0:
            normal_file = PROCESSED_FOLDER / f"processed_{timestamp}.csv"
            df_normal.to_csv(normal_file, index=False)
            print(f"Saved normal transactions to: {normal_file}")

        if len(df_suspicious) > 0:
            suspicious_file = SUSPICIOUS_FOLDER / f"suspicious_{timestamp}.csv"
            df_suspicious.to_csv(suspicious_file, index=False)
            print(f"WARNING: Saved suspicious transactions to: {suspicious_file}")

        print(f"Batch processing completed successfully")

    except NotImplementedError as e:
        print(f"WARNING: Skipping processing: {e}")
    except Exception as e:
        print(f"ERROR: Error processing batch: {e}")


def main():
    """Main loop - generates and processes transactions every minute"""
    print("="*60)
    print("Transaction Processing Pipeline")
    print("="*60)

    setup_folders()

    print(f"\nStarting continuous processing (every {INTERVAL_SECONDS} seconds)")
    print("Press Ctrl+C to stop\n")

    batch_count = 0

    try:
        while True:
            batch_count += 1
            print(f"\n{'='*60}")
            print(f"BATCH #{batch_count}")
            print(f"{'='*60}")

            # Generate new transactions
            raw_file = generate_batch()

            # Process the batch
            process_batch(raw_file)

            # Wait for next interval
            print(f"\nWaiting {INTERVAL_SECONDS} seconds until next batch...")
            time.sleep(INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\n\nPipeline stopped by user")
        print(f"Total batches processed: {batch_count}")


if __name__ == "__main__":
    main()
