import pandas as pd
from sqlalchemy import create_engine

def create_engine_db(db_name=None):
    user = 'me'
    password = '0000'
    host = 'localhost'
    if db_name:
        url = f'mysql+mysqlconnector://{user}:{password}@{host}/{db_name}'
    else:
        url = f'mysql+mysqlconnector://{user}:{password}@{host}'
    return create_engine(url)

def extract_data(query, engine):
    try:
        with engine.connect() as conn:
            data = pd.read_sql(query, conn)
            print(f"Data extracted for query: {query}")
            return data
    except Exception as e:
        print(f"Error extracting data: {e}")
        return pd.DataFrame()

def transform_dim_vehicles(trucks):
    print("Transforming DimVehicles...")
    return trucks[['matricule', 'model', 'year', 'type', 'Mileage', 'status']].rename(columns={
        'matricule': 'vehicle_id',
        'model': 'model',
        'year': 'year',
        'type': 'type',
        'Mileage': 'mileage',
        'status': 'status'
    })

def transform_vehicle_maintenance_fact(trucks, time_dim):
    print("Transforming VehicleMaintenanceFact...")

    # Ensure the dates are in the same format
    trucks['last_maintenance_date'] = pd.to_datetime(trucks['last_maintenance_date'], errors='coerce').dt.date
    trucks['next_maintenance_date'] = pd.to_datetime(trucks['next_maintenance_date'], errors='coerce').dt.date
    trucks['last_repared_at'] = pd.to_datetime(trucks['last_repared_at'], errors='coerce').dt.date

    time_dim['date_key'] = pd.to_datetime(time_dim['date_key']).dt.date

    # Merge to get date_key from time_dim based on date fields
    trucks = trucks.merge(time_dim[['date_key']], left_on='last_maintenance_date', right_on='date_key', how='left').rename(columns={'date_key': 'last_maintenance_date_key'})
    trucks = trucks.merge(time_dim[['date_key']], left_on='next_maintenance_date', right_on='date_key', how='left').rename(columns={'date_key': 'next_maintenance_date_key'})
    trucks = trucks.merge(time_dim[['date_key']], left_on='last_repared_at', right_on='date_key', how='left').rename(columns={'date_key': 'last_repared_at_key'})

    vehicle_maintenance_fact = trucks[['vehicle_id', 'last_maintenance_date_key', 'next_maintenance_date_key', 'last_repared_at_key']].copy()

    return vehicle_maintenance_fact

def load_data(df, table_name, engine, create_table_query=None, unique_columns=None):
    try:
        if create_table_query:
            with engine.connect() as conn:
                conn.execute(create_table_query)
                print(f"Table {table_name} created or already exists.")

        if unique_columns:
            # Extract existing data to check for duplicates
            query = f"SELECT {', '.join(unique_columns)} FROM {table_name}"
            existing_data = pd.read_sql(query, engine)

            # Merge to find duplicates and exclude them
            df = df.merge(existing_data, on=unique_columns, how='left', indicator=True)
            df = df[df['_merge'] == 'left_only'].drop(columns=['_merge'])

        df.to_sql(table_name.lower(), engine, if_exists='append' if create_table_query else 'replace', index=False)
        print(f"Data loaded into {table_name} successfully.")
    except Exception as e:
        print(f"Error loading data into {table_name}: {e}")

def main_etl_process():
    print("Starting ETL process...")

    source_engine = create_engine_db('pfe')
    destination_engine = create_engine_db('dwh3')

    # Extract trucks data
    trucks_query = "SELECT * FROM trucks"
    trucks = extract_data(trucks_query, source_engine)

    # Extract and load the Time Dimension table
    time_dim_query = "SELECT * FROM time_dimension"
    time_dim = extract_data(time_dim_query, destination_engine)

    # Transform data and create vehicle dimension table
    dim_vehicles = transform_dim_vehicles(trucks)
    create_dim_vehicles_table_query = """
        CREATE TABLE IF NOT EXISTS dim_vehicles (
            vehicle_id VARCHAR(255) PRIMARY KEY,
            model VARCHAR(255),
            year INT,
            type VARCHAR(255),
            mileage INT,
            status VARCHAR(255)
        )
    """
    load_data(dim_vehicles, 'dim_vehicles', destination_engine, create_dim_vehicles_table_query)

    # Transform data and create vehicle maintenance fact table
    vehicle_maintenance_fact = transform_vehicle_maintenance_fact(trucks, time_dim)
    create_vehicle_maintenance_fact_table_query = """
        CREATE TABLE IF NOT EXISTS vehicle_maintenance_fact (
            vehicle_id VARCHAR(255),
            last_maintenance_date_key DATE,
            next_maintenance_date_key DATE,
            last_repared_at_key DATE,
            FOREIGN KEY (vehicle_id) REFERENCES dim_vehicles(vehicle_id),
            FOREIGN KEY (last_maintenance_date_key) REFERENCES time_dimension(date_key),
            FOREIGN KEY (next_maintenance_date_key) REFERENCES time_dimension(date_key),
            FOREIGN KEY (last_repared_at_key) REFERENCES time_dimension(date_key)
        )
    """
    load_data(vehicle_maintenance_fact, 'vehicle_maintenance_fact', destination_engine, create_vehicle_maintenance_fact_table_query, unique_columns=['vehicle_id', 'last_maintenance_date_key', 'next_maintenance_date_key', 'last_repared_at_key'])

    print("ETL process for Vehicle Maintenance completed.")

if __name__ == "__main__":
    main_etl_process()
