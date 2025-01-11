
import requests
import pandas as pd
import pyodbc
from scipy import stats

# Define the data path url and database connection details
data_url = 'https://raw.githubusercontent.com/easyAppx/Lab6_ETL_Project/refs/heads/main/Lab6ETL.csv'
server = 'OBIORA\\INSTANCE_ONE_SQL'
database = 'Lab6ETL'

# Fetch data from GitHub API
def fetch_data():
    try:
        response = requests.get(data_url)
        response.raise_for_status()  # Raise an error for bad responses
        print("Data fetched successfully from GitHub API URL.")
        return response.content
    except Exception as e:
        print("Error fetching data from GitHub API URL:", e)
        return None


# Save the raw CSV data to a file
def save_data_to_csv(data):
    csv_file_path = 'population_data.csv'
    with open(csv_file_path, 'wb') as file:
        file.write(data)
    print("Data saved to CSV file successfully.")
    return csv_file_path


# Load data into DataFrame
def load_data(csv_file_path):
    try:
        df = pd.read_csv(csv_file_path)
        print("Data loaded successfully into DataFrame.")
        print(df.head())  # Displays the first 5 rows
        print("\nData Types and Summary Information:")
        print(df.info())  # Provides a summary of columns and data types
        return df
    except Exception as e:
        print("Error loading data into DataFrame:", e)
        return None


# Establish a connection to the database
def connect_to_db():
    try:
        conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID=;PWD=') 
        cursor = conn.cursor()
        print("Database connection established.")
        return conn, cursor
    except Exception as e:
        print("Error connecting to database:", e)
        return None, None


# Create table if not exists
def create_table(cursor, table_name, create_table_query):
    try:
        cursor.execute(create_table_query)
        print(f"Table '{table_name}' created successfully.")
    except Exception as e:
        print(f"Error creating table '{table_name}':", e)


# Insert data into a table
def insert_data(cursor, table_name, df, query):
    try:
        for index, row in df.iterrows():
            cursor.execute(query, tuple(row))
        cursor.connection.commit()
        print(f"Data inserted into '{table_name}' successfully.")
    except Exception as e:
        print(f"Error inserting data into '{table_name}':", e)

# Data Cleaning Function
def clean_data(df):
    df = df.dropna()  # Drop any rows with Null values
    print("Data cleaned by removing rows with null values.")
    return df

# Menu to choose the tasks
def menu():
    df = None
    while True:
        print("\nSelect a task to perform:")
        print("1. Fetch and Load Data")
        print("2. Data Cleaning")
        print("3. Filter Data for 2020")
        print("4. Standardize Age Group Names")
        print("5. Remove Outliers")
        print("6. Aggregate Deaths by Country Name")
        print("7. Exit")

        choice = input("Enter your choice: ")

        if choice == '1':
            data = fetch_data()
            if data:
                csv_file_path = save_data_to_csv(data)
                df = load_data(csv_file_path)
                if df is not None:
                    print("Data loaded and ready for further processing.")
        elif choice == '2':
            if df is not None:
                # df_no_nulls = df.dropna()
                df = clean_data(df)
                conn, cursor = connect_to_db()
                if conn and cursor:
                    create_table(cursor, 'CleanedData_NoNulls', '''
                        CREATE TABLE CleanedData_NoNulls (
                            [Region_Code] NVARCHAR(50),
                            [Region_Name] NVARCHAR(50),
                            [Country_Code] NVARCHAR(50),
                            [Country_Name] NVARCHAR(50),
                            [Year] INT,
                            [Sex] NVARCHAR(10),
                            [Age_Group_Code] NVARCHAR(50),
                            [Age_Group] NVARCHAR(50),
                            [Number] INT,
                            [Percentage of cause-specific deaths out of total deaths] FLOAT,
                            [Age-standardized death rate per 100 000 standard population] FLOAT,
                            [Death rate per 100 000 population] FLOAT
                        )
                    ''')
                    insert_data(cursor, 'CleanedData_NoNulls', df, '''
                        INSERT INTO CleanedData_NoNulls VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''')
                    cursor.close()
                    conn.close()
            else:
                print("Data not loaded. Please fetch the data first.")
        elif choice == '3':
            if df is not None:
                df_2020 = df[df['Year'] == 2020]
                conn, cursor = connect_to_db()
                if conn and cursor:
                    create_table(cursor, 'CleanedData_2020', '''
                        CREATE TABLE CleanedData_2020 (
                            [Region_Code] NVARCHAR(50),
                            [Region_Name] NVARCHAR(50),
                            [Country_Code] NVARCHAR(50),
                            [Country_Name] NVARCHAR(50),
                            [Year] INT,
                            [Sex] NVARCHAR(10),
                            [Age_Group_Code] NVARCHAR(50),
                            [Age_Group] NVARCHAR(50),
                            [Number] INT,
                            [Percentage of cause-specific deaths out of total deaths] FLOAT,
                            [Age-standardized death rate per 100 000 standard population] FLOAT,
                            [Death rate per 100 000 population] FLOAT
                        )
                    ''')
                    insert_data(cursor, 'CleanedData_2020', df_2020, '''
                        INSERT INTO CleanedData_2020 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''')
                    cursor.close()
                    conn.close()
            else:
                print("Data not loaded. Please fetch the data first.")
        elif choice == '4':
            if df is not None:
                df['Age Group'] = df['Age Group'].str.lower().str.strip()
                conn, cursor = connect_to_db()
                if conn and cursor:
                    create_table(cursor, 'CleanedData_AgeGroup', '''
                        CREATE TABLE CleanedData_AgeGroup (
                            [Region_Code] NVARCHAR(50),
                            [Region_Name] NVARCHAR(50),
                            [Country_Code] NVARCHAR(50),
                            [Country_Name] NVARCHAR(50),
                            [Year] INT,
                            [Sex] NVARCHAR(10),
                            [Age_Group_Code] NVARCHAR(50),
                            [Age_Group] NVARCHAR(50),
                            [Number] INT,
                            [Percentage of cause-specific deaths out of total deaths] FLOAT,
                            [Age-standardized death rate per 100 000 standard population] FLOAT,
                            [Death rate per 100 000 population] FLOAT
                        )
                    ''')
                    insert_data(cursor, 'CleanedData_AgeGroup', df, '''
                        INSERT INTO CleanedData_AgeGroup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''')
                    cursor.close()
                    conn.close()
            else:
                print("Data not loaded. Please fetch the data first.")
        elif choice == '5':
            if df is not None:
                mean_death_rate = df['Death rate per 100 000 population'].mean()
                std_death_rate = df['Death rate per 100 000 population'].std()
                z_score_threshold = 3
                df_no_outliers = df[
                    (df['Death rate per 100 000 population'] >= mean_death_rate - z_score_threshold * std_death_rate) &
                    (df['Death rate per 100 000 population'] <= mean_death_rate + z_score_threshold * std_death_rate)
                ]
                print("Outliers removed...")
                conn, cursor = connect_to_db()
                if conn and cursor:
                    create_table(cursor, 'CleanedData_WithoutOutliers', '''
                        CREATE TABLE CleanedData_WithoutOutliers (
                            [Region_Code] NVARCHAR(50),
                            [Region_Name] NVARCHAR(50),
                            [Country_Code] NVARCHAR(50),
                            [Country_Name] NVARCHAR(50),
                            [Year] INT,
                            [Sex] NVARCHAR(10),
                            [Age_Group_Code] NVARCHAR(50),
                            [Age_Group] NVARCHAR(50),
                            [Number] INT,
                            [Percentage of cause-specific deaths out of total deaths] FLOAT,
                            [Age-standardized death rate per 100 000 standard population] FLOAT,
                            [Death rate per 100 000 population] FLOAT
                        )
                    ''')
                    insert_data(cursor, 'CleanedData_WithoutOutliers', df_no_outliers, '''
                        INSERT INTO CleanedData_WithoutOutliers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''')
                    cursor.close()
                    conn.close()
            else:
                print("Data not loaded. Please fetch the data first.")
        elif choice == '6':
            if df is not None:
                df_aggregated = df.groupby('Country Name').agg({'Number': 'sum'}).reset_index()
                df_aggregated = df_aggregated.sort_values(by='Number', ascending=False)
                print(df_aggregated)
                    # Connect to the database
                conn, cursor = connect_to_db()
                if conn and cursor:
                    # Create the CleanedData_Aggregated table
                    create_table(cursor, 'CleanedData_Aggregated', '''
                        CREATE TABLE CleanedData_Aggregated (
                            [Country_Name] NVARCHAR(50),
                            [Total_Deaths] INT
                        )
                    ''')

                    # Insert the aggregated data into the CleanedData_Aggregated table
                    insert_data(cursor, 'CleanedData_Aggregated', df_aggregated, '''
                        INSERT INTO CleanedData_Aggregated (Country_Name, Total_Deaths) VALUES (?, ?)
                    ''')

                    cursor.close()
                    conn.close()
                    print("Aggregated data loaded into 'CleanedData_Aggregated' table successfully.")
                else:
                    print("Database connection failed.")
            else:
                print("Data not loaded. Please fetch the data first.")
        elif choice == '7':
            print("Exiting the program.")
            break
        else:
            print("Invalid choice. Please try again.")

# Run the menu function
menu()