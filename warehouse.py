import pandas as pd
import datetime
from sqlalchemy import create_engine, text

# Create a SQLAlchemy engine
def create_engine_db(db_name=None):
    user = 'me'
    password = '0000'
    host = 'localhost'
    if db_name:
        url = f'mysql+mysqlconnector://{user}:{password}@{host}/{db_name}'
    else:
        url = f'mysql+mysqlconnector://{user}:{password}@{host}'
    return create_engine(url)

# Extract Data
def extract_data(query, engine):
    try:
        with engine.connect() as conn:
            data = pd.read_sql(query, conn)
            print(f"Data extracted for query: {query}")
            return data
    except Exception as e:
        print(f"Error extracting data: {e}")
        return pd.DataFrame()

# Transformation Functions
def transform_dim_users(users):
    print("Transforming DimUsers...")
    return users[['idusers', 'Username', 'Mail', 'Role', 'created_at']].rename(columns={
        'idusers': 'user_id',
        'Mail': 'email',
        'created_at': 'hire_date'
    })

def transform_dim_vehicles(trucks):
    print("Transforming DimVehicles...")
    return trucks[['matricule', 'model', 'year', 'type', 'last_maintenance_date', 'next_maintenance_date', 'last_repared_at', 'status']].rename(columns={
        'matricule': 'vehicle_id',
        'next_maintenance_date': 'next_maintenance_date',
        'last_maintenance_date': 'last_maintenance_date',
        'last_repared_at': 'last_repaired_date',
         'status': 'status'   
    })

def transform_fact_driver_tasks(driver_tasks, rehla):
    print("Transforming FactDriverTasks...")
    driver_tasks_with_km = driver_tasks.merge(
        rehla[['id_D', 'date', 'KM']],
        left_on=['id_driver', 'date'],
        right_on=['id_D', 'date'],
        how='left'
    )
    driver_tasks_with_km['KM'] = driver_tasks_with_km['KM'].fillna(0)
    fact_driver_tasks = driver_tasks_with_km[['idtask', 'id_driver', 'matricule', 'date', 'KM']].rename(columns={
        'idtask': 'task_id',
        'id_driver': 'driver_id',
        'matricule': 'vehicle_id',
        'date': 'task_date',
        'KM': 'km_covered'
    })
    return fact_driver_tasks

def transform_fact_mecano_tasks(mecano_tasks):
    print("Transforming FactMecanoTasks...")
    return mecano_tasks[['idmecano_tasks', 'id_mecano', 'matricule', 'date', 'done']].rename(columns={
        'idmecano_tasks': 'task_id',
        'id_mecano': 'mecano_id',
        'matricule': 'vehicle_id',
        'date': 'repair_date',
        'done': 'task_done'
    })


def transform_fact_vehicle_maintenance(trucks, mecano_tasks):
    print("Transforming FactVehicleMaintenance...")

    # Print the first few rows to understand the structure
    print("Trucks DataFrame:")
    print(trucks.head())

    print("MecanoTasks DataFrame:")
    print(mecano_tasks.head())

    # Group by vehicle_id and count the number of maintenance tasks
    maintenance_count = mecano_tasks.groupby('matricule').size().reset_index(name='maintenance_count')
    print("Maintenance Count DataFrame:")
    print(maintenance_count.head())

    # Merge trucks with maintenance counts
    fact_vehicle_maintenance = trucks.merge(maintenance_count, left_on='matricule', right_on='matricule', how='left')
    
    # Handle NaNs
    fact_vehicle_maintenance['maintenance_count'] = fact_vehicle_maintenance['maintenance_count'].fillna(0)
    fact_vehicle_maintenance['next_maintenance_date'] = fact_vehicle_maintenance['next_maintenance_date'].replace(0, pd.NaT)
    fact_vehicle_maintenance['last_maintenance_date'] = fact_vehicle_maintenance['last_maintenance_date'].replace(0, pd.NaT)
    fact_vehicle_maintenance['last_repared_at'] = fact_vehicle_maintenance['last_repared_at'].replace(0, pd.NaT)

    print("FactVehicleMaintenance DataFrame after merge and handling NaNs:")
    print(fact_vehicle_maintenance.head())

    return fact_vehicle_maintenance[['matricule', 'next_maintenance_date', 'last_maintenance_date', 'last_repared_at', 'maintenance_count']].rename(columns={
        'matricule': 'vehicle_id',
        'next_maintenance_date': 'next_maintenance_date',
        'last_maintenance_date': 'last_maintenance_date',
        'last_repared_at': 'last_repaired_date'
    })


def generate_date_range(start_date, end_date):
    print(f"Generating date range from {start_date} to {end_date}")
    dates = pd.date_range(start_date, end_date).to_pydatetime().tolist()
    dim_dates = pd.DataFrame(dates, columns=['date'])
    dim_dates['day'] = dim_dates['date'].dt.day
    dim_dates['month'] = dim_dates['date'].dt.month
    dim_dates['year'] = dim_dates['date'].dt.year
    dim_dates['quarter'] = dim_dates['date'].dt.quarter
    dim_dates['day_of_week'] = dim_dates['date'].dt.dayofweek
    dim_dates['day_name'] = dim_dates['date'].dt.day_name()
    return dim_dates.rename(columns={'date': 'date_key'})

# Loading Function
def load_data(df, table_name, engine):
    create_table_queries = {
    'DimUsers': """
        CREATE TABLE DimUsers (
            user_id INT PRIMARY KEY,
            username VARCHAR(255),
            email VARCHAR(255),
            role VARCHAR(255),
            hire_date DATETIME
        )
    """,
    'DimVehicles': """
        CREATE TABLE DimVehicles (
            vehicle_id VARCHAR(255) PRIMARY KEY,
            model VARCHAR(255),
            year INT,
            type VARCHAR(255),
            next_maintenance_date DATETIME,
            last_maintenance_date DATETIME,
            last_repaired_date DATETIME,
            status VARCHAR(255)
        )
    """,
    'FactDriverTasks': """
        CREATE TABLE FactDriverTasks (
            task_id VARCHAR(255) PRIMARY KEY,
            driver_id INT,
            vehicle_id VARCHAR(255),
            task_date DATETIME,
            km_covered FLOAT
        )
    """,
    'FactMecanoTasks': """
        CREATE TABLE FactMecanoTasks (
            task_id VARCHAR(255) PRIMARY KEY,
            mecano_id INT,
            vehicle_id VARCHAR(255),
            repair_date DATETIME,
            task_done VARCHAR(255)  -- Adding the task_done field
        )
    """,
    'FactVehicleMaintenance': """
        CREATE TABLE FactVehicleMaintenance (
            vehicle_id VARCHAR(255) PRIMARY KEY,
            next_maintenance_date DATETIME,
            last_maintenance_date DATETIME,
            last_repaired_date DATETIME,
            maintenance_count INT
        )
    """,
    'DimDates': """
        CREATE TABLE DimDates (
            date_key DATE PRIMARY KEY,
            day INT,
            month INT,
            year INT,
            quarter INT,
            day_of_week INT,
            day_name VARCHAR(255)
        )
    """
}


    try:
        with engine.connect() as conn:
            print(f"Creating table '{table_name}'...")
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            conn.execute(text(create_table_queries[table_name]))
            print(f"Table '{table_name}' created successfully.")
            df.to_sql(table_name, conn, if_exists='append', index=False)
            print(f"Table '{table_name}' loaded successfully.")
    except Exception as e:
        print(f"Error loading data into table '{table_name}': {e}")

# ETL Process with Date Dimension
def etl_process():
    try:
        # Connect to source database (pfe)
        source_engine = create_engine_db('pfe')

        # Extract data from source database
        users = extract_data("SELECT * FROM users", source_engine)
        trucks = extract_data("SELECT * FROM trucks", source_engine)
        driver_tasks = extract_data("SELECT * FROM driver_tasks", source_engine)
        mecano_tasks = extract_data("SELECT * FROM mecano_tasks", source_engine)
        rehla = extract_data("SELECT * FROM rehla", source_engine)

        # Transform data
        dim_users = transform_dim_users(users)
        dim_vehicles = transform_dim_vehicles(trucks)
        fact_driver_tasks = transform_fact_driver_tasks(driver_tasks, rehla)
        fact_mecano_tasks = transform_fact_mecano_tasks(mecano_tasks)
        fact_vehicle_maintenance = transform_fact_vehicle_maintenance(trucks, mecano_tasks)

        # Generate date range for DimDates table
        min_date = min(driver_tasks['date'].min(), mecano_tasks['date'].min(), rehla['date'].min())
        max_date = max(driver_tasks['date'].max(), mecano_tasks['date'].max(), rehla['date'].max())
        dim_dates = generate_date_range(min_date, max_date)

        # Connect to destination database (pfe_dw)
        destination_engine = create_engine_db('sawekji2')

        # Load data into destination database
        load_data(dim_users, 'DimUsers', destination_engine)
        load_data(dim_vehicles, 'DimVehicles', destination_engine)
        load_data(fact_driver_tasks, 'FactDriverTasks', destination_engine)
        load_data(fact_mecano_tasks, 'FactMecanoTasks', destination_engine)
        load_data(fact_vehicle_maintenance, 'FactVehicleMaintenance', destination_engine)
        load_data(dim_dates, 'DimDates', destination_engine)

        print("ETL process completed successfully.")

    except Exception as e:
        print(f"ETL process failed: {e}")

# Run the ETL process
etl_process()