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
    return users[['idusers', 'Username', 'Mail', 'created_at', 'Role', 'last_activity_at', 'status']].rename(columns={
        'idusers': 'user_id',
        'Username': 'username',
        'Mail': 'email',
        'created_at': 'hire_date',
        'Role': 'role',
        'last_activity_at': 'last_time_active',
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

def create_user_management_fact(dim_users, time_dim):
    print("Creating User Management Fact Table...")

    dim_users['last_time_active'] = pd.to_datetime(dim_users['last_time_active'], errors='coerce').dt.date
    dim_users.dropna(subset=['last_time_active'], inplace=True)

    # Ensure the dates are in the same format
    time_dim['date_key'] = pd.to_datetime(time_dim['date_key']).dt.date

    # Merge to get date_key from time_dim based on last_time_active
    dim_users = dim_users.merge(time_dim[['date_key']], left_on='last_time_active', right_on='date_key', how='left', suffixes=('', '_last_active'))

    user_management_fact = dim_users[['user_id', 'date_key', 'status', 'hire_date']].copy()
    user_management_fact.rename(columns={'date_key': 'last_time_active_date'}, inplace=True)

    return user_management_fact

def calculate_measures(dim_users, user_management_fact):
    print("Calculating measures for User Management Fact Table...")

    # Calculate activity counts
    activity_count = user_management_fact.groupby('user_id').size().reset_index(name='activity_count')
    user_management_fact = user_management_fact.merge(activity_count, on='user_id', how='left')

    # Calculate active and inactive users count
    user_management_fact['active_users_count'] = user_management_fact['status'].apply(lambda x: 1 if x == 'active' else 0)
    user_management_fact['inactive_users_count'] = user_management_fact['status'].apply(lambda x: 1 if x == 'inactive' else 0)

    # Calculate new users count
    new_users_count = dim_users.groupby('hire_date').size().reset_index(name='new_users_count')
    user_management_fact = user_management_fact.merge(new_users_count, left_on='hire_date', right_on='hire_date', how='left')

    # Calculate churned users count
    churned_users_count = user_management_fact.groupby(['user_id', 'status']).size().reset_index(name='churned_users_count')
    user_management_fact = user_management_fact.merge(churned_users_count, on='user_id', how='left')

    # Remove duplicate status columns
    if 'status_x' in user_management_fact.columns:
        user_management_fact['status'] = user_management_fact['status_x']
        user_management_fact.drop(columns=['status_x', 'status_y'], inplace=True)

    # Calculate retention rate
    user_management_fact['retention_rate'] = user_management_fact['active_users_count'] / (user_management_fact['active_users_count'] + user_management_fact['churned_users_count'])

    # Calculate average time between activities
    user_management_fact['last_time_active_date'] = pd.to_datetime(user_management_fact['last_time_active_date'])
    user_management_fact['avg_time_between_activities'] = user_management_fact.groupby('user_id')['last_time_active_date'].diff().dt.total_seconds().mean()

    # Calculate average user activity per day
    avg_activity_per_day = user_management_fact.groupby('last_time_active_date')['activity_count'].mean().reset_index(name='avg_activity_per_day')
    user_management_fact = user_management_fact.merge(avg_activity_per_day, on='last_time_active_date', how='left')

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
    users['created_at'] = pd.to_datetime(users['created_at'], errors='coerce').dt.date

    # Create and load the DimUsers table
    dim_users = transform_dim_users(users)
    create_dim_users_table_query = """
        CREATE TABLE IF NOT EXISTS dimusers (
            user_id INT PRIMARY KEY,
            username VARCHAR(255),
            email VARCHAR(255),
            hire_date DATE,
            role VARCHAR(255),
            last_time_active DATE,
            status VARCHAR(255)
        )
    """
    load_data(dim_users, 'dimusers', destination_engine, create_dim_users_table_query)

    # Create and load the Time Dimension table
    start_date = dim_users['hire_date'].min()
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
    user_management_fact = create_user_management_fact(dim_users, time_dim)
    user_management_fact = calculate_measures(dim_users, user_management_fact)
    create_user_management_fact_table_query = """
        CREATE TABLE IF NOT EXISTS user_management_fact (
            user_id INT,
            last_time_active_date DATE,
            status VARCHAR(255),
            hire_date DATE,
            activity_count INT,
            active_users_count INT,
            inactive_users_count INT,
            new_users_count INT,
            churned_users_count INT,
            retention_rate FLOAT,
            avg_time_between_activities FLOAT,
            avg_activity_per_day FLOAT,
            engagement_score FLOAT,
            FOREIGN KEY (user_id) REFERENCES dimusers(user_id),
            FOREIGN KEY (last_time_active_date) REFERENCES time_dimension(date_key)
        )
    """
    load_data(user_management_fact, 'user_management_fact', destination_engine, create_user_management_fact_table_query)

    print("ETL process for User Management completed.")

if __name__ == "__main__":
    main_etl_process()
