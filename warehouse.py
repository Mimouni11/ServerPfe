import pandas as pd
import datetime
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

def load_data(df, table_name, engine, create_table_query):
    try:
        with engine.connect() as conn:
            conn.execute(create_table_query)
            print(f"Table {table_name} created or already exists.")

        print(f"DataFrame info for {table_name}:")
        print(df.info())
        print(f"Checking for NaN values in {table_name}:")
        print(df.isnull().sum())

        df.to_sql(table_name.lower(), engine, if_exists='append', index=False)
        print(f"Data loaded into {table_name} successfully.")
    except Exception as e:
        print(f"Error loading data into {table_name}: {e}")

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
    destination_engine = create_engine_db('dwh2')

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
    start_date = datetime.date(2000, 1, 1)
    end_date = pd.to_datetime('today').date()

    time_dim = generate_date_range(start_date, end_date)
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
    load_data(user_management_fact, 'user_management_fact', destination_engine, create_user_management_fact_table_query)

    print("ETL process for User Management completed.")

if __name__ == "__main__":
    main_etl_process()
