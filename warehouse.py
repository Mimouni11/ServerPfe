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


def transform_dim_vehicles(trucks):
    print("Transforming DimVehicles...")
    print(trucks.head())  # Debug print to check the initial data structure
    dim_vehicles = trucks[['matricule', 'model', 'year', 'type', 'Mileage', 'status']].rename(columns={
        'matricule': 'vehicle_id',
        'model': 'model',
        'year': 'year',
        'type': 'type',
        'Mileage': 'mileage',
        'status': 'status'
    })
    print(dim_vehicles.head())  # Debug print to check the transformed data structure
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

def generate_date_range(users):
    print("Generating date range based on data...")

    min_date = users[['created_at', 'last_activity_at']].min().min()
    end_date = pd.to_datetime('today').date()

    print(f"Minimum date in data: {min_date}")

    dates = pd.date_range(min_date, end_date).to_pydatetime().tolist()
    dim_dates = pd.DataFrame(dates, columns=['date'])
    dim_dates['day'] = dim_dates['date'].dt.day
    dim_dates['month'] = dim_dates['date'].dt.month
    dim_dates['year'] = dim_dates['date'].dt.year
    dim_dates['quarter'] = dim_dates['date'].dt.quarter
    dim_dates['day_of_week'] = dim_dates['date'].dt.dayofweek
    dim_dates['day_name'] = dim_dates['date'].dt.day_name()

    return dim_dates.rename(columns={'date': 'date_key'})




def transform_vehicle_maintenance_fact(trucks, time_dim):
    print("Transforming VehicleMaintenanceFact...")

    # Ensure the dates are in datetime format
    trucks['last_maintenance_date'] = pd.to_datetime(trucks['last_maintenance_date'], errors='coerce')
    trucks['next_maintenance_date'] = pd.to_datetime(trucks['next_maintenance_date'], errors='coerce')
    trucks['last_repared_at'] = pd.to_datetime(trucks['last_repared_at'], errors='coerce')

    time_dim['date_key'] = pd.to_datetime(time_dim['date_key'])
    trucks.rename(columns={'matricule': 'vehicle_id'}, inplace=True)

    # Merge to get date_key from time_dim based on date fields
    trucks = trucks.merge(time_dim[['date_key', 'day']], left_on='last_maintenance_date', right_on='date_key', how='left').rename(columns={'date_key': 'last_maintenance_date_key', 'day': 'last_maintenance_day'})
    trucks = trucks.merge(time_dim[['date_key', 'day']], left_on='next_maintenance_date', right_on='date_key', how='left').rename(columns={'date_key': 'next_maintenance_date_key', 'day': 'next_maintenance_day'})
    trucks = trucks.merge(time_dim[['date_key', 'day']], left_on='last_repared_at', right_on='date_key', how='left').rename(columns={'date_key': 'last_repared_at_key', 'day': 'last_repared_day'})

    # Calculate measures
    trucks['maintenance_duration'] = (trucks['next_maintenance_date'] - trucks['last_maintenance_date']).dt.days
    trucks['days_since_last_repair'] = (pd.to_datetime('today') - trucks['last_repared_at']).dt.days
    trucks['is_overdue'] = trucks['next_maintenance_date'] < pd.to_datetime('today')
    trucks['repair_count'] = trucks.groupby('vehicle_id')['last_repared_at'].transform('count')

    vehicle_maintenance_fact = trucks[['vehicle_id', 'last_maintenance_date_key', 'next_maintenance_date_key', 'last_repared_at_key', 'maintenance_duration', 'days_since_last_repair', 'is_overdue', 'repair_count']].copy()

    # Debug print to check the final data structure
    print(vehicle_maintenance_fact.head())

    return vehicle_maintenance_fact

def create_user_management_fact(users, dim_users, time_dim):
    print("Creating User Management Fact Table...")

    users['created_at'] = pd.to_datetime(users['created_at'], errors='coerce').dt.date
    users['last_activity_at'] = pd.to_datetime(users['last_activity_at'], errors='coerce').dt.date
    users.dropna(subset=['created_at', 'last_activity_at'], inplace=True)

    # Ensure the dates are in the same format
    time_dim['date_key'] = pd.to_datetime(time_dim['date_key']).dt.date

    # Merge to get date_key from time_dim based on created_at and last_activity_at
    users = users.merge(time_dim[['date_key']], left_on='created_at', right_on='date_key', how='left').rename(columns={'date_key': 'hire_date'})
    users = users.merge(time_dim[['date_key']], left_on='last_activity_at', right_on='date_key', how='left').rename(columns={'date_key': 'last_time_active'})

    # Merge to get user_id from dim_users
    users = users.merge(dim_users[['user_id', 'username']], left_on='Username', right_on='username', how='left')

    user_management_fact = users[['user_id', 'hire_date', 'last_time_active', 'status']].copy()

    return user_management_fact

def calculate_measures(user_management_fact):
    print("Calculating measures for User Management Fact Table...")

    # Calculate activity counts
    activity_count = user_management_fact.groupby('user_id').size().reset_index(name='activity_count')
    user_management_fact = user_management_fact.merge(activity_count, on='user_id', how='left')

    # Calculate active and inactive users count
    user_management_fact['active_users_count'] = user_management_fact['status'].apply(lambda x: 1 if x == 'active' else 0)
    user_management_fact['inactive_users_count'] = user_management_fact['status'].apply(lambda x: 1 if x == 'inactive' else 0)

    # Remove duplicate status columns
    if 'status_x' in user_management_fact.columns:
        user_management_fact['status'] = user_management_fact['status_x']
        user_management_fact.drop(columns=['status_x', 'status_y'], inplace=True)

    # Calculate retention rate
    user_management_fact['retention_rate'] = user_management_fact['active_users_count'] / (user_management_fact['active_users_count'] + user_management_fact['inactive_users_count'])

    # Calculate user engagement score
    user_management_fact['engagement_score'] = user_management_fact['activity_count'] * 0.5 + user_management_fact['retention_rate'] * 0.3

    return user_management_fact

def main_etl_process():
    print("Starting ETL process...")

    source_engine = create_engine_db('pfe')
    destination_engine = create_engine_db('dwh3')

    # Extract and transform tasks data
    mecano_tasks_query = "SELECT * FROM mecano_tasks"
    driver_tasks_query = "SELECT * FROM driver_tasks"
    mecano_tasks = extract_data(mecano_tasks_query, source_engine)
    driver_tasks = extract_data(driver_tasks_query, source_engine)
    trucks_query = "SELECT * FROM trucks"
    trucks = extract_data(trucks_query, source_engine)
    task_dimension = transform_dim_tasks(driver_tasks, mecano_tasks)

    # Define the create table query for task_dimension
    create_task_dimension_table_query = """
        CREATE TABLE IF NOT EXISTS task_dimension (
            task_id VARCHAR(255) PRIMARY KEY,
            description TEXT,
            task_for VARCHAR(255),
            task_type VARCHAR(255),
            done varchar(8)
        )
    """
    
    # Load data into task dimension table in dwh3 data warehouse
    load_data(task_dimension, 'task_dimension', destination_engine, create_task_dimension_table_query)
    # Extract and transform users data
    users_query = "SELECT * FROM users"
    users = extract_data(users_query, source_engine)

    # Create and load the DimUsers table
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

    # Create and load the Time Dimension table
    time_dim = generate_date_range(users)
    create_time_dim_table_query = """
        CREATE TABLE IF NOT EXISTS time_dimension (
            date_key DATE PRIMARY KEY,
            day INT,
            month INT,
            year INT,
            quarter INT,
            day_of_week INT,
            day_name VARCHAR(255)
        )
    """
    load_data(time_dim, 'time_dimension', destination_engine, create_time_dim_table_query)

   
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





      # Create and load the User Management Fact Table
    user_management_fact = create_user_management_fact(users, dim_users, time_dim)
    user_management_fact = calculate_measures(user_management_fact)
    create_user_management_fact_table_query = """
        CREATE TABLE IF NOT EXISTS user_management_fact (
            user_id INT,
            hire_date DATE,
            last_time_active DATE,
            status VARCHAR(255),
            activity_count INT,
            active_users_count INT,
            inactive_users_count INT,
            retention_rate FLOAT,
            engagement_score FLOAT,
            FOREIGN KEY (user_id) REFERENCES dimusers(user_id),
            FOREIGN KEY (hire_date) REFERENCES time_dimension(date_key),
            FOREIGN KEY (last_time_active) REFERENCES time_dimension(date_key)
        )
    """
    load_data(user_management_fact, 'user_management_fact', destination_engine, create_user_management_fact_table_query, unique_columns=['user_id', 'hire_date', 'last_time_active'])
    
    # Transform data and create vehicle maintenance fact table
    vehicle_maintenance_fact = transform_vehicle_maintenance_fact(trucks, time_dim)
    create_vehicle_maintenance_fact_table_query = """
    CREATE TABLE IF NOT EXISTS vehicle_maintenance_fact (
        vehicle_id VARCHAR(255),
        last_maintenance_date_key DATE,
        next_maintenance_date_key DATE,
        last_repared_at_key DATE,
        maintenance_duration INT,
        days_since_last_repair INT,
        is_overdue BOOLEAN,
        repair_count INT,
        FOREIGN KEY (vehicle_id) REFERENCES dim_vehicles(vehicle_id),
        FOREIGN KEY (last_maintenance_date_key) REFERENCES time_dimension(date_key),
        FOREIGN KEY (next_maintenance_date_key) REFERENCES time_dimension(date_key),
        FOREIGN KEY (last_repared_at_key) REFERENCES time_dimension(date_key)
    )
"""
    load_data(vehicle_maintenance_fact, 'vehicle_maintenance_fact', destination_engine, create_vehicle_maintenance_fact_table_query, unique_columns=['vehicle_id', 'last_maintenance_date_key', 'next_maintenance_date_key', 'last_repared_at_key'])

    

    print("ETL process for User Management completed.")

if __name__ == "__main__":
    main_etl_process()
