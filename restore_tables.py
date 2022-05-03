import pandas as pd
import os
from fastavro import reader
import config
import create_tables

root_path = './static/backups/'
engine = config.generate_engine()  # Generating engine


def main():

    # Moving main table to be last
    files = os.listdir(root_path)
    last_files = [file for file in files if 'hired_employees' in file]
    first_files = [file for file in files if 'hired_employees' not in file]
    final_files = first_files + last_files

    if not final_files:
        print('There are no backed tables to use in restoring process')

    else:
        print('Initializing restoration process')
        # Cleaning tables
        create_tables.clean_tables()

        # Reading Avro Files
        for file in final_files:
            avro_records = list()
            with open(f'{root_path}{file}', 'rb') as fo:
                avro_reader = reader(fo)
                for record in avro_reader:
                    avro_records.append(record)

            df_avro = pd.DataFrame(avro_records)

            filename = file.rsplit('_', 1)[0]

            # Saving dataframe by chunks of 1000
            for df_chunk_data in split_dataframe(df_avro):
                df_chunk_data.to_sql(name=filename, con=engine, if_exists='append', index=False)

            print(f'Table {filename} was successfully restored')

        print('Restoration process finished')


def split_dataframe(df, chunk_size=1000):
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks


if __name__ == '__main__':
    main()
