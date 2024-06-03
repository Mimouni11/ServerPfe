import pandas as pd
import datetime
from sqlalchemy import create_engine, insert,MetaData, Table

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


# Transformation Functions
def transform_dim_users(users):
    print("Transforming DimUsers...")
    return users[['idusers', 'Username', 'Mail', 'created_at']].rename(columns={
        'idusers': 'user_id',
        'Username': 'username',
        'Mail': 'email',
        'created_at': 'hire_date'
    })

def transform_dim_vehicles(trucks):
    print("Transforming DimVehicles...")
    return trucks[['matricule', 'model', 'year', 'type', 'last_maintenance_date', 'next_maintenance_date']].rename(columns={
        'matricule': 'vehicle_id',
        'model': 'model',
        'year': 'year',
        'type': 'type',
        'last_maintenance_date': 'last_maintenance_date',
        'next_maintenance_date': 'next_maintenance_date'
    })

def transform_dim_drivers(drivers):
    print("Transforming DimDrivers...")
    return drivers[['id', 'username']].rename(columns={
        'id': 'driver_id',
        'username': 'username'
    })

def transform_dim_destinations(rehla):
    print("Transforming DimDestinations...")
    destinations = rehla['destinations'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).reset_index(name='destination')
    destinations = destinations.drop_duplicates().reset_index(drop=True)
    destinations['destination_id'] = destinations.index + 1
    return destinations[['destination_id', 'destination']].rename(columns={'destination': 'destination_name'})

def transform_fact_driver_tasks(driver_tasks, rehla, dim_destinations):
    print("Transforming FactDriverTasks...")

    # Ensure date columns are in datetime format
    driver_tasks['date'] = pd.to_datetime(driver_tasks['date'], errors='coerce')
    rehla['date'] = pd.to_datetime(rehla['date'], errors='coerce')

    # Convert id columns to appropriate types and remove leading/trailing spaces
    driver_tasks['idtask'] = driver_tasks['idtask'].astype(str).str.strip()
    driver_tasks['id_driver'] = driver_tasks['id_driver'].astype(int)
    rehla['id_D'] = rehla['id_D'].astype(int)

    # Merge driver_tasks with rehla, allowing for unmatched rows
    driver_tasks_with_details = driver_tasks.merge(
        rehla[['id_D', 'date', 'destinations', 'KM']],
        left_on=['id_driver', 'date'],
        right_on=['id_D', 'date'],
        how='left'
    )

    # Fill missing values in KM with 0
    driver_tasks_with_details['KM'] = driver_tasks_with_details['KM'].fillna(0)

    # Map destinations to IDs
    destination_mapping = dim_destinations.set_index('destination_name')['destination_id'].to_dict()

    def get_destination_ids(destinations):
        if pd.isna(destinations):
            return None
        dest_list = [d.strip() for d in destinations.split(',')]
        return [destination_mapping.get(d, None) for d in dest_list if d in destination_mapping]

    # Apply destination ID mapping
    driver_tasks_with_details['destination_ids'] = driver_tasks_with_details['destinations'].apply(get_destination_ids)
    driver_tasks_exploded = driver_tasks_with_details.explode('destination_ids').dropna(subset=['destination_ids'])

    # Create the fact table
    fact_driver_tasks = driver_tasks_exploded[['idtask', 'id_driver', 'matricule', 'date', 'KM', 'destination_ids']].rename(columns={
        'idtask': 'task_id',
        'id_driver': 'driver_id',
        'matricule': 'vehicle_id',
        'date': 'task_date',
        'KM': 'km_covered',
        'destination_ids': 'destination_id'
    })

    print("Transformed FactDriverTasks DataFrame:")
    print(fact_driver_tasks.head())
    print(f"Number of rows in FactDriverTasks: {len(fact_driver_tasks)}")

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
    date_columns = ['next_maintenance_date', 'last_maintenance_date']
    for col in date_columns:
        trucks[col] = pd.to_datetime(trucks[col], errors='coerce')
    maintenance_count = mecano_tasks.groupby('matricule').size().reset_index(name='maintenance_count')
    fact_vehicle_maintenance = trucks.merge(maintenance_count, left_on='matricule', right_on='matricule', how='left')
    fact_vehicle_maintenance['maintenance_count'] = fact_vehicle_maintenance['maintenance_count'].fillna(0).astype(int)
    return fact_vehicle_maintenance[['matricule', 'next_maintenance_date', 'last_maintenance_date']].rename(columns={
        'matricule': 'vehicle_id',
        'next_maintenance_date': 'next_maintenance_date',
        'last_maintenance_date': 'last_maintenance_date'
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

def load_data(df, table_name, engine):
    create_table_queries = {
        'dimusers': """
            CREATE TABLE dimusers (
                user_id INT PRIMARY KEY,
                username VARCHAR(255),
                email VARCHAR(255),
                hire_date DATETIME
            )
        """,
        'dimvehicles': """
            CREATE TABLE dimvehicles (
                vehicle_id VARCHAR(255) PRIMARY KEY,
                model VARCHAR(255),
                year INT,
                type VARCHAR(255),
                last_maintenance_date DATETIME,
                next_maintenance_date DATETIME
            )
        """,
        'dimdrivers': """
            CREATE TABLE dimdrivers (
                driver_id INT PRIMARY KEY,
                username VARCHAR(255)
            )
        """,
        'dimdestinations': """
            CREATE TABLE dimdestinations (
                destination_id INT PRIMARY KEY,
                destination_name VARCHAR(255)
            )
        """,
        'factdrivertasks': """
            CREATE TABLE factdrivertasks (
                task_id VARCHAR(255) PRIMARY KEY,
                driver_id INT,
                vehicle_id VARCHAR(255),
                task_date DATETIME,
                km_covered FLOAT,
                destination_id INT,
                FOREIGN KEY (driver_id) REFERENCES dimdrivers(driver_id),
                FOREIGN KEY (vehicle_id) REFERENCES dimvehicles(vehicle_id),
                FOREIGN KEY (destination_id) REFERENCES dimdestinations(destination_id)
            )
        """,
        'factmecanotasks': """
            CREATE TABLE factmecanotasks (
                task_id VARCHAR(255) PRIMARY KEY,
                mecano_id INT,
                vehicle_id VARCHAR(255),
                repair_date DATETIME,
                task_done VARCHAR(6),
                FOREIGN KEY (mecano_id) REFERENCES dimusers(user_id),
                FOREIGN KEY (vehicle_id) REFERENCES dimvehicles(vehicle_id)
            )
        """,
        'factvehiclemaintenance': """
            CREATE TABLE factvehiclemaintenance (
                vehicle_id VARCHAR(255) PRIMARY KEY,
                next_maintenance_date DATETIME,
                last_maintenance_date DATETIME,
                maintenance_count INT,
                FOREIGN KEY (vehicle_id) REFERENCES dimvehicles(vehicle_id)
            )
        """,
        'dimdates': """
            CREATE TABLE dimdates (
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
            if not engine.dialect.has_table(conn, table_name.lower()):
                conn.execute(create_table_queries[table_name.lower()])
                print(f"Table {table_name} created.")
            else:
                print(f"Table {table_name} already exists.")

        print(f"DataFrame info for {table_name}:")
        print(df.info())
        print(f"Checking for NaN values in {table_name}:")
        print(df.isnull().sum())

        df.to_sql(table_name.lower(), engine, if_exists='append', index=False)
        print(f"Data loaded into {table_name} successfully.")
    except Exception as e:
        print(f"Error loading data into {table_name}: {e}")

def load_data_with_ignore(df, table_name, engine):
    try:
        with engine.connect() as conn:
            metadata = MetaData(bind=engine)
            table = Table(table_name, metadata, autoload_with=engine)

            insert_stmt = insert(table).prefix_with('IGNORE')
            conn.execute(insert_stmt, df.to_dict(orient='records'))

        print(f"Data loaded into {table_name} successfully with IGNORE.")
    except Exception as e:
        print(f"Error loading data into {table_name} with IGNORE: {e}")

def main_etl_process():
    print("Starting ETL process...")

    source_engine = create_engine_db('pfe')
    destination_engine = create_engine_db('dwh')

    users_query = "SELECT * FROM users"
    trucks_query = "SELECT * FROM trucks"
    driver_tasks_query = "SELECT * FROM driver_tasks"
    mecano_tasks_query = "SELECT * FROM mecano_tasks"
    rehla_query = "SELECT * FROM rehla"
    drivers_query = "SELECT * FROM drivers"

    users = extract_data(users_query, source_engine)
    trucks = extract_data(trucks_query, source_engine)
    driver_tasks = extract_data(driver_tasks_query, source_engine)
    mecano_tasks = extract_data(mecano_tasks_query, source_engine)
    rehla = extract_data(rehla_query, source_engine)
    drivers = extract_data(drivers_query, source_engine)

    users['created_at'] = pd.to_datetime(users['created_at'], errors='coerce')
    trucks['last_maintenance_date'] = pd.to_datetime(trucks['last_maintenance_date'], errors='coerce')
    trucks['next_maintenance_date'] = pd.to_datetime(trucks['next_maintenance_date'], errors='coerce')
    mecano_tasks['date'] = pd.to_datetime(mecano_tasks['date'], errors='coerce')
    rehla['date'] = pd.to_datetime(rehla['date'], errors='coerce')

    dim_users = transform_dim_users(users)
    dim_vehicles = transform_dim_vehicles(trucks)
    dim_drivers = transform_dim_drivers(drivers)
    dim_destinations = transform_dim_destinations(rehla)
    fact_driver_tasks = transform_fact_driver_tasks(driver_tasks, rehla, dim_destinations)
    fact_mecano_tasks = transform_fact_mecano_tasks(mecano_tasks)
    fact_vehicle_maintenance = transform_fact_vehicle_maintenance(trucks, mecano_tasks)

    min_date = min(users['created_at'].min(), trucks['last_maintenance_date'].min(), trucks['next_maintenance_date'].min(), mecano_tasks['date'].min(), rehla['date'].min())
    max_date = datetime.datetime.now()
    dim_dates = generate_date_range(min_date, max_date)

    load_data(dim_users, 'DimUsers', destination_engine)
    load_data(dim_vehicles, 'DimVehicles', destination_engine)
    load_data(dim_drivers, 'DimDrivers', destination_engine)
    load_data(dim_destinations, 'DimDestinations', destination_engine)
    print("FactDriverTasks DataFrame to be loaded:")
    print(fact_driver_tasks.head())
    print(f"Number of rows to be loaded into FactDriverTasks: {len(fact_driver_tasks)}")
    load_data_with_ignore(fact_driver_tasks, 'FactDriverTasks', destination_engine)
    load_data(fact_mecano_tasks, 'FactMecanoTasks', destination_engine)
    load_data(fact_vehicle_maintenance, 'FactVehicleMaintenance', destination_engine)
    load_data(dim_dates, 'DimDates', destination_engine)

    print("ETL process completed.")

if __name__ == "__main__":
    main_etl_process()
