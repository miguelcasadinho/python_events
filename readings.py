import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Accessing environment variables
user = os.getenv('psqlGiggoUser')
password = os.getenv('psqlGiggoPassword')
host = os.getenv('psqlGiggoHost')
port = os.getenv('psqlGiggoPort')
database = os.getenv('psqlGiggoDatabase')

# Create a SQLAlchemy engine
connection_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
engine = create_engine(connection_string)

# Establish a connection to the PostgreSQL database
try:
    with engine.connect() as connection:
        #print("Connection to PostgreSQL DB successful")

        # Define your query
        query = """
            SELECT 
                ROW_NUMBER() OVER () AS "Não.",
                resultado_final."Cliente",
                resultado_final."Morada",
                resultado_final."ID de Medidor",
                resultado_final."ID do Consumidor",
                resultado_final."N/S Medidor",
                resultado_final."ID Imovel",
                resultado_final."Leitura1",
                resultado_final."Ultima Leitura"
            FROM (
                SELECT DISTINCT ON (device)
                    REPLACE(infocontrato.name, ',', ' ') AS "Cliente",
                    CONCAT(infocontrato.street, ' ', infocontrato.num_pol, ' ', infocontrato.floor) AS "Morada",
                    infocontrato.local AS "ID de Medidor",
                    infocontrato.client AS "ID do Consumidor",
                    device AS "N/S Medidor",
                    clients.building AS "ID Imovel",
                    volume AS "Leitura1",
                    date AS "Ultima Leitura"            
                FROM 
                    volume
                LEFT JOIN (
                    SELECT 
                        infocontrato.local,
                        infocontrato.client,
                        infocontrato.name,
                        infocontrato.street,
                        infocontrato.num_pol,
                        infocontrato.floor,
                        infocontrato.device AS dev
                    FROM 
                        infocontrato
                ) AS infocontrato ON volume.device = infocontrato.dev
                LEFT JOIN (
                    SELECT 
                        clients.sensitivity, 
                        clients.building, 
                        clients.ramal, 
                        clients.client,
                        clients.situation
                    FROM 
                        clients
                ) AS clients ON infocontrato.client = clients.client
                LEFT JOIN (
                    SELECT 
                        ramaisrua.zmc, 
                        ramaisrua.ramal AS ram 
                    FROM 
                        ramaisrua
                ) AS ramaisrua ON clients.ramal = ramaisrua.ram
                WHERE
                    volume.date > NOW() - INTERVAL '15 days'
                    AND (clients.situation != 'LIQUIDADO' AND clients.situation != 'ANULADO')
                    AND device != '8868310'
                ORDER BY 
                    device, 
                    date DESC
            ) AS resultado_final;
        """  

        # Use pandas to read the SQL query into a DataFrame
        df = pd.read_sql_query(query, connection)

         # Ensure 'Ultima Leitura' column is in UTC before formatting
        df['Ultima Leitura'] = pd.to_datetime(df['Ultima Leitura'], utc=True).dt.strftime('%d/%m/%Y %H:%M')

        # Convert 'Leitura1' column to integer with zero decimals
        df['Leitura1'] = df['Leitura1'].astype(int)

        # Display the DataFrame
        #print(df)

        # Generate a dynamic filename with the current date and time
        timestamp = datetime.now().strftime('%d%m%Y')
        output_file = f'/home/giggo/nodejs/events/readings_{timestamp}.csv'

       # Save the DataFrame to a CSV file
        df.to_csv(output_file, encoding='utf-16', sep='\t', index=False)
        print(f"readings_{timestamp}.csv")

except Exception as e:
    import traceback
    print("An error occurred:")
    traceback.print_exc()  # Print the full stack trace for debugging

#finally:
    # The connection is automatically closed when the context manager exits
    #print("PostgreSQL connection closed")