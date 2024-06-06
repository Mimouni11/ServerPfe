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

def transform_dim_users(users):
    print("Transforming DimUsers...")
    return users[['idusers', 'Username', 'Mail', 'Role', 'status']].rename(columns={
        'idusers': 'user_id',
        'Username': 'username',
        'Mail': 'email',
        'Role': 'role',
        'status': 'status'
    })

def transform_dim_destinations(rehla):
    print("Transforming DimDestinations...")

    # Split destinations by comma and stack them vertically
    destinations = rehla['destinations'].str.split(',', expand=True).stack().reset_index(drop=True).rename('destination')
    
    # Create a DataFrame from the stacked destinations
    dim_destinations = pd.DataFrame(destinations)
    
    return dim_destinations

def transform_dim_vehicles(trucks):
    print("Transforming DimVehicles...")
    dim_vehicles = trucks[['matricule', 'model', 'year', 'type', 'Mileage', 'status']].rename(columns={
        'matricule': 'vehicle_id',
        'model': 'model',
        'year': 'year',
        'type': 'type',
        'Mileage': 'mileage',
        'status': 'status'
    })
    return dim_vehicles

def transform_dim_tasks(driver_tasks, mecano_tasks):
    print("Transforming DimTasks...")

    # Add 'task_for' field to differentiate between mecano and driver tasks
    driver_tasks['task_for'] = 'driver'
    driver_tasks['task_type'] = '-'  # Set task_type to "-" for driver tasks
    mecano_tasks['task_for'] = 'mecano'

    # Rename task ID columns to a common name
    driver_tasks.rename(columns={'idtask': 'task_id', 'task': 'description'}, inplace=True)
    mecano_tasks.rename(columns={'idmecano_tasks': 'task_id', 'tasks': 'description', 'task_type': 'task_type'}, inplace=True)

    # Combine driver and mecano tasks
    tasks = pd.concat([driver_tasks, mecano_tasks])

    return tasks[['task_id', 'description', 'task_for', 'task_type', 'done']]

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

        if create_table_query:
            print(f"DataFrame info for {table_name}:")
            print(df.info())
            print(f"Checking for NaN values in {table_name}:")
            print(df.isnull().sum())

        df.to_sql(table_name.lower(), engine, if_exists='append' if create_table_query else 'replace', index=False)
        print(f"Data loaded into {table_name} successfully.")
    except Exception as e:
        print(f"Error loading data into {table_name}: {e}")

def create_driver_fact_table(users, rehla, tasks, time_dim):
    print("Creating DriverFactTable...")

    # Filter users to get only drivers
    drivers = users[users['role'] == 'driver']
    
    # Extract relevant data from rehla
    rehla_data = rehla[['id_R', 'id_D', 'date', 'destinations', 'id_task']]

    # Merge drivers with rehla data
    driver_rehla = drivers.merge(rehla_data, left_on='user_id', right_on='id_D')

    # Split destinations and explode them
    driver_rehla = driver_rehla.assign(destination=driver_rehla['destinations'].str.split(',')).explode('destination')

    # Ensure 'date' columns are of datetime type
    driver_rehla['date'] = pd.to_datetime(driver_rehla['date'])
    time_dim['date'] = pd.to_datetime(time_dim['date'])

    # If time_dim doesn't have date_id, create it
    if 'date_id' not in time_dim.columns:
        time_dim['date_id'] = time_dim.index

    # Merge with time_dim to get date keys
    driver_fact = driver_rehla.merge(time_dim, left_on='date', right_on='date', how='left')

    # Debug print to check columns after merge
    print("Columns after merging with time_dim:", driver_fact.columns)

    # Merge with tasks using id_task and task_id
    driver_fact = driver_fact.merge(tasks, left_on='id_task', right_on='task_id', how='left')

    # Debug print to check columns after merging with tasks
    print("Columns after merging with tasks:", driver_fact.columns)

    # Select relevant columns
    driver_fact = driver_fact[['user_id', 'date_id', 'destination', 'task_id']]

    return driver_fact




def create_time_dim(engine):
    print("Creating TimeDim table...")

    create_time_dim_table_query = """
        CREATE TABLE IF NOT EXISTS time_dim (
            date_id INT AUTO_INCREMENT PRIMARY KEY,
            date DATE,
            year INT,
            month INT,
            day INT,
            weekday VARCHAR(10),
            is_weekend BOOLEAN
        )
    """
    with engine.connect() as conn:
        conn.execute(create_time_dim_table_query)
    
    time_dim = pd.DataFrame({
        'date': pd.date_range(start='2024-01-01', end='2024-12-31'),
    })
    time_dim['year'] = time_dim['date'].dt.year
    time_dim['month'] = time_dim['date'].dt.month
    time_dim['day'] = time_dim['date'].dt.day
    time_dim['weekday'] = time_dim['date'].dt.day_name()
    time_dim['is_weekend'] = time_dim['weekday'].isin(['Saturday', 'Sunday'])

    load_data(time_dim, 'time_dim', engine)

def main_etl_process():
    print("Starting ETL process...")

    source_engine = create_engine_db('pfe')
    destination_engine = create_engine_db('dwh5')

    # Extract and transform users data
    users_query = "SELECT * FROM users"
    users = extract_data(users_query, source_engine)
    dim_users = transform_dim_users(users)
    create_dim_users_table_query = """
        CREATE TABLE IF NOT EXISTS dimusers (
            user_id INT PRIMARY KEY,
            username VARCHAR(255),
            email VARCHAR(255),
            role VARCHAR(255),
            status VARCHAR(255)
        )
    """
    load_data(dim_users, 'dimusers', destination_engine, create_dim_users_table_query)

    # Extract and transform rehla data
    rehla_query = "SELECT * FROM rehla"
    rehla = extract_data(rehla_query, source_engine)
    dim_destinations = transform_dim_destinations(rehla)
    create_dim_destinations_table_query = """
        CREATE TABLE IF NOT EXISTS dim_destinations (
            destination_id INT PRIMARY KEY AUTO_INCREMENT,
            destination VARCHAR(255)
        )
    """
    load_data(dim_destinations, 'dim_destinations', destination_engine, create_dim_destinations_table_query)

    # Extract and transform trucks data
    trucks_query = "SELECT * FROM trucks"
    trucks = extract_data(trucks_query, source_engine)
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

    # Extract and transform tasks data
    mecano_tasks_query = "SELECT * FROM mecano_tasks"
    driver_tasks_query = "SELECT * FROM driver_tasks"
    mecano_tasks = extract_data(mecano_tasks_query, source_engine)
    driver_tasks = extract_data(driver_tasks_query, source_engine)
    task_dimension = transform_dim_tasks(driver_tasks, mecano_tasks)
    create_task_dimension_table_query = """
        CREATE TABLE IF NOT EXISTS task_dimension (
            task_id VARCHAR(255) PRIMARY KEY,
            description TEXT,
            task_for VARCHAR(255),
            task_type VARCHAR(255),
            done VARCHAR(8)
        )
    """
    load_data(task_dimension, 'task_dimension', destination_engine, create_task_dimension_table_query)

    # Check if time_dim table exists and create if not
    create_time_dim(destination_engine)

    # Extract and transform time_dim data
    time_dim_query = "SELECT * FROM time_dim"
    time_dim = extract_data(time_dim_query, destination_engine)

    # Create DriverFactTable
    driver_fact = create_driver_fact_table(dim_users, rehla, task_dimension, time_dim)
    create_driver_fact_table_query = """
        CREATE TABLE IF NOT EXISTS driver_fact_table (
            user_id INT,
            date_id INT,
            destination VARCHAR(255),
            task_id VARCHAR(255),
            PRIMARY KEY (user_id, date_id, destination, task_id)
        )
    """
    load_data(driver_fact, 'driver_fact_table', destination_engine, create_driver_fact_table_query)

if __name__ == "__main__":
    main_etl_process()
