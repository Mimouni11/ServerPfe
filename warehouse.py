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

    # Merge to get date_key from time_dim based on date
    dim_users = dim_users.merge(time_dim[['date_key']], left_on='last_time_active', right_on='date_key', how='left')

    user_management_fact = dim_users[['user_id', 'date_key', 'status']].copy()

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
    create_user_management_fact_table_query = """
        CREATE TABLE IF NOT EXISTS user_management_fact (
            user_id INT,
            date_key DATE,
            status VARCHAR(255),
            FOREIGN KEY (user_id) REFERENCES dimusers(user_id),
            FOREIGN KEY (date_key) REFERENCES time_dimension(date_key)
        )
    """
    load_data(user_management_fact, 'user_management_fact', destination_engine, create_user_management_fact_table_query)

    print("ETL process for User Management completed.")

if __name__ == "__main__":
    main_etl_process()
