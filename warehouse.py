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

def generate_date_range_for_data_warehouse(relevant_tables):
    print("Generating date range for the data warehouse...")

    all_dates = []

    # Iterate over relevant tables to find the minimum and maximum dates
    for table in relevant_tables:
        date_columns = [col for col in table.columns if col.endswith('_at') or col.endswith('_date')]
        if date_columns:
            min_date = table[date_columns].min().min()
            max_date = table[date_columns].max().max()
            all_dates.extend(pd.date_range(min_date, max_date).tolist())

    min_date = min(all_dates)
    max_date = max(all_dates)
    
    print(f"Minimum date in data warehouse: {min_date}")
    print(f"Maximum date in data warehouse: {max_date}")

    dates = pd.date_range(min_date, max_date).to_pydatetime().tolist()
    dim_dates = pd.DataFrame(dates, columns=['date'])
    dim_dates['day'] = dim_dates['date'].dt.day
    dim_dates['month'] = dim_dates['date'].dt.month
    dim_dates['year'] = dim_dates['date'].dt.year
    dim_dates['quarter'] = dim_dates['date'].dt.quarter
    dim_dates['day_of_week'] = dim_dates['date'].dt.dayofweek
    dim_dates['day_name'] = dim_dates['date'].dt.day_name()

    return dim_dates.rename(columns={'date': 'date_key'})







def transform_fact_mecanotask(dim_users, mecano_tasks, time_dim, dim_vehicles, task_dim):
    print("Transforming FactMecanotask...")

    # Filter mechanics from dim_users table
    mechanics = dim_users[dim_users['role'] == 'mecano'][['user_id', 'username']]
    print("Mechanics:")
    print(mechanics.head())

    # Ensure mecano_tasks has unique entries
    mecano_tasks = mecano_tasks.drop_duplicates(subset=['id_mecano', 'task_id'])
    print("Unique Mecano Tasks:")
    print(mecano_tasks.head())

    # Merge mechanics with mecano_tasks
    fact_mecanotask = mecano_tasks.merge(mechanics, left_on='id_mecano', right_on='user_id', how='inner')
    print("After merging mechanics with mecano_tasks:")
    print(fact_mecanotask.head())

    # Convert task date to datetime and merge with time_dim
    fact_mecanotask['date'] = pd.to_datetime(fact_mecanotask['date'], errors='coerce').dt.date
    time_dim['date_key'] = pd.to_datetime(time_dim['date_key']).dt.date
    fact_mecanotask = fact_mecanotask.merge(time_dim[['date_key']], left_on='date', right_on='date_key', how='left')
    print("After merging with time_dim:")
    print(fact_mecanotask.head())

    # Merge with dim_vehicles to get the vehicle_id and model
    fact_mecanotask = fact_mecanotask.merge(dim_vehicles[['vehicle_id', 'model']], left_on='matricule', right_on='vehicle_id', how='left')
    print("After merging with dim_vehicles:")
    print(fact_mecanotask.head())

    # Ensure task_dim has unique entries
    task_dim = task_dim.drop_duplicates(subset=['task_id'])
    print("Unique Task Dimension:")
    print(task_dim.head())

    # Merge with task_dim to get the 'done' attribute
    fact_mecanotask = fact_mecanotask.merge(task_dim[['task_id', 'done']], left_on='task_id', right_on='task_id', how='left')
    print("After merging with task_dim:")
    print(fact_mecanotask.head())

    # Calculate total tasks per mechanic (both done and not done)
    total_tasks_per_mecano = fact_mecanotask.groupby('user_id').size().reset_index(name='total_tasks')
    print("Total tasks per mechanic (all):")
    print(total_tasks_per_mecano.head())

    # Calculate total done tasks per mechanic
    total_done_tasks_per_mecano = fact_mecanotask[fact_mecanotask['done_y'] == 'yes'].groupby('user_id').size().reset_index(name='total_done_tasks')
    print("Total done tasks per mechanic:")
    print(total_done_tasks_per_mecano.head())

    # Merge the total tasks and total done tasks back into fact_mecanotask
    fact_mecanotask = fact_mecanotask.merge(total_tasks_per_mecano, on='user_id', how='left')
    fact_mecanotask = fact_mecanotask.merge(total_done_tasks_per_mecano, on='user_id', how='left')

    # Calculate the percentage of tasks done
    fact_mecanotask['total_done_tasks'] = fact_mecanotask['total_done_tasks'].fillna(0)
    fact_mecanotask['percentage_tasks_done'] = (fact_mecanotask['total_done_tasks'] / fact_mecanotask['total_tasks']) * 100

    # Calculate total tasks done per vehicle model for each mechanic
    tasks_done_per_model = fact_mecanotask[fact_mecanotask['done_y'] == 'yes'].groupby(['user_id', 'model_y']).size().reset_index(name='tasks_done_per_model')
    print("Total tasks done per vehicle model for each mechanic:")
    print(tasks_done_per_model.head())

    # Calculate the total tasks per model for each mechanic
    total_tasks_per_model = fact_mecanotask.groupby(['user_id', 'model_y']).size().reset_index(name='total_tasks_per_model')

    # Merge tasks done per model with total tasks per model
    tasks_per_model = tasks_done_per_model.merge(total_tasks_per_model, on=['user_id', 'model_y'], how='left')

    # Calculate the percentage of tasks done per model
    tasks_per_model['percentage_tasks_done_per_model'] = (tasks_per_model['tasks_done_per_model'] / tasks_per_model['total_tasks_per_model']) * 100

    print("Tasks per model calculations:")
    print(tasks_per_model.head())

    # Merge these calculations back into the main DataFrame
    fact_mecanotask = fact_mecanotask.merge(tasks_per_model, on=['user_id', 'model_y'], how='left')

    # Select relevant columns for the final fact table
    fact_mecanotask = fact_mecanotask[['user_id', 'task_id', 'date_key', 'vehicle_id', 'total_tasks', 'total_done_tasks', 'percentage_tasks_done', 'tasks_done_per_model', 'total_tasks_per_model', 'percentage_tasks_done_per_model']].rename(columns={
        'date_key': 'task_date_key'
    })

    # Ensure final DataFrame has no duplicates
    fact_mecanotask = fact_mecanotask.drop_duplicates()
    print("Final columns in fact_mecanotask:")
    print(fact_mecanotask.columns)

    return fact_mecanotask















def transform_vehicle_maintenance_fact(trucks, time_dim):
    print("Transforming VehicleMaintenanceFact...")

    # Convert dates to datetime in temporary variables for calculations
    last_maintenance_date_dt = pd.to_datetime(trucks['last_maintenance_date'], errors='coerce')
    next_maintenance_date_dt = pd.to_datetime(trucks['next_maintenance_date'], errors='coerce')
    last_repared_at_dt = pd.to_datetime(trucks['last_repared_at'], errors='coerce')

    # Calculate maintenance interval and time since last repair using datetime calculations
    trucks['maintenance_interval'] = (next_maintenance_date_dt - last_maintenance_date_dt).dt.days
    trucks['time_since_last_repair'] = (pd.to_datetime('today') - last_repared_at_dt).dt.days

    # Merge to get date_key from time_dim based on date fields
    time_dim['date_key'] = pd.to_datetime(time_dim['date_key']).dt.date
    trucks = trucks.merge(time_dim[['date_key']], left_on='last_maintenance_date', right_on='date_key', how='left').rename(columns={'date_key': 'last_maintenance_date_key'})
    trucks = trucks.merge(time_dim[['date_key']], left_on='next_maintenance_date', right_on='date_key', how='left').rename(columns={'date_key': 'next_maintenance_date_key'})
    trucks = trucks.merge(time_dim[['date_key']], left_on='last_repared_at', right_on='date_key', how='left').rename(columns={'date_key': 'last_repared_at_key'})

    # Rename 'matricule' to 'vehicle_id'
    trucks.rename(columns={'matricule': 'vehicle_id'}, inplace=True)

    vehicle_maintenance_fact = trucks[['vehicle_id', 'last_maintenance_date_key', 'next_maintenance_date_key', 'last_repared_at_key', 'maintenance_interval', 'time_since_last_repair']].copy()

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
    destination_engine = create_engine_db('dwh4')

    # Extract and transform tasks data
    mecano_tasks_query = "SELECT * FROM mecano_tasks"
    driver_tasks_query = "SELECT * FROM driver_tasks"
    mecano_tasks = extract_data(mecano_tasks_query, source_engine)
    driver_tasks = extract_data(driver_tasks_query, source_engine)
    trucks_query = "SELECT * FROM trucks"
    trucks = extract_data(trucks_query, source_engine)
    task_dimension = transform_dim_tasks(driver_tasks, mecano_tasks)
# Extract and transform rehla data
    rehla_query = "SELECT * FROM rehla"
    rehla = extract_data(rehla_query, source_engine)
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
    relevant_tables = [users, driver_tasks, trucks]  # List of relevant tables in your data warehouse
    time_dim = generate_date_range_for_data_warehouse(relevant_tables)
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

    dim_destinations = transform_dim_destinations(rehla)
    create_dim_destinations_table_query = """
CREATE TABLE IF NOT EXISTS dim_destinations (
    destination_id INT PRIMARY KEY AUTO_INCREMENT,
    destination VARCHAR(255)
)
"""
    load_data(dim_destinations, 'dim_destinations', destination_engine, create_dim_destinations_table_query)  



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
    print("ETL process for User Management completed.")
    # Transform data and create vehicle maintenance fact table
    vehicle_maintenance_fact = transform_vehicle_maintenance_fact(trucks, time_dim)
    create_vehicle_maintenance_fact_table_query = """
        CREATE TABLE IF NOT EXISTS vehicle_maintenance_fact (
            vehicle_id VARCHAR(255),
            last_maintenance_date_key DATE,
            next_maintenance_date_key DATE,
            last_repared_at_key DATE,
            maintenance_interval INT,
            time_since_last_repair INT,
            FOREIGN KEY (vehicle_id) REFERENCES dim_vehicles(vehicle_id),
            FOREIGN KEY (last_maintenance_date_key) REFERENCES time_dimension(date_key),
            FOREIGN KEY (next_maintenance_date_key) REFERENCES time_dimension(date_key),
            FOREIGN KEY (last_repared_at_key) REFERENCES time_dimension(date_key)
        )
    """
    load_data(vehicle_maintenance_fact, 'vehicle_maintenance_fact', destination_engine, create_vehicle_maintenance_fact_table_query, unique_columns=['vehicle_id', 'last_maintenance_date_key', 'next_maintenance_date_key', 'last_repared_at_key'])

   # Transform fact_mecanotask
    fact_mecanotask = transform_fact_mecanotask(dim_users, mecano_tasks, time_dim, dim_vehicles,task_dimension)

    # Create table for fact_mecanotask and load data
    create_fact_mecanotask_table_query = """
    CREATE TABLE IF NOT EXISTS fact_mecanotask (
    user_id INT,
    task_id VARCHAR(255),
    task_date_key DATE,
    vehicle_id VARCHAR(255),
    total_tasks INT,
    total_done_tasks INT,
    percentage_tasks_done FLOAT,
    tasks_done_per_model INT,
    total_tasks_per_model INT,
    percentage_tasks_done_per_model FLOAT,
    FOREIGN KEY (user_id) REFERENCES dimusers(user_id),
    FOREIGN KEY (task_id) REFERENCES task_dimension(task_id),
    FOREIGN KEY (task_date_key) REFERENCES time_dimension(date_key),
    FOREIGN KEY (vehicle_id) REFERENCES dim_vehicles(vehicle_id)
)

    """
    load_data(fact_mecanotask, 'fact_mecanotask', destination_engine, create_fact_mecanotask_table_query, unique_columns=['user_id', 'task_id', 'task_date_key', 'vehicle_id'])

    print("ETL process for fact_mecanotask completed.") 

   

if __name__ == "__main__":
    main_etl_process()