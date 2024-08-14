import pandas as pd
import numpy as np
import subprocess

def ratings_df(file_path):
    # Create a DatFrame for Movie Ratings
    ratings_dataFrame = pd.read_csv(file_path, delimiter='\t', header=None, names=['userID','itemID', 'rating' ,'timestamp'])
    ratings_dataFrame.sort_values(by='itemID', ascending=True, inplace=True)
    return ratings_dataFrame

def movies_df(file_path):
    # Create a DataFrame for Movie Names
    with open(file_path, 'r') as file:
        
        counter = 0
        movies_dataFrame = pd.DataFrame(columns=['itemID', 'movieName', 'release_timestamp'])
        lines = file.readlines()
        # Iterate thorugh the lines in the file

        for line in lines:
            # From each line extract the first three values
            fields = line.split('|')
            itemID, movieName, release_timestamp = fields[0:3]
            movieName = movieName[0:len(movieName) - 6]

            # Aggerate line data
            line_data = [int(itemID), str(movieName), release_timestamp]

            # Create a temp dataframe and append it to movies_dataFrame
            temp_df = pd.DataFrame(data=[line_data], columns=['itemID', 'movieName', 'release_timestamp'])
            movies_dataFrame = pd.concat([temp_df, movies_dataFrame], ignore_index=True)

            counter += 1

        # Sort Values by item id
        movies_dataFrame.sort_values(by='itemID', ascending=True, inplace=True)
    # Close file
    file.close()

    return movies_dataFrame

def etlDataFrame():
    ratingsDataFrame = ratings_df('ml-100k/u.data')
    moviesDataFrame = movies_df('ml-100k/u.item')
    etlDataFrame = pd.merge(moviesDataFrame, ratingsDataFrame, on='itemID')
    return etlDataFrame

def export_dataframe_to_csv(dataframe, csv_name):

    try:
        # Export to CSV
        dataframe.to_csv("./"+csv_name + ".csv", index=False)
    except Exception as e:
        print(e)

def etl_csv():
    etl_dataFrame = etlDataFrame()
    export_dataframe_to_csv(etl_dataFrame,"ETL")

def load_file():
    # Define your Docker container name and paths
    container_name = "pubsub_emulator"
    local_file_path = "./ETL.csv"
    container_dir_path = "/path/in/container"
    container_file_path = f"{container_dir_path}/ETL.csv"

    # Step 1: Run the container with the volume mounted
    subprocess.run([
        "docker", "run", "--rm", "-d", "-p", "8085:8085", "--name", container_name,
        "-v", f"{local_file_path}:/tmp/ETL.csv",
        "google/cloud-sdk:emulators",
        "/bin/bash", "-c", "gcloud beta emulators pubsub start --project=some-project-id --host-port='0.0.0.0:8085'"
    ])

    # Step 2: Create the directory inside the container
    subprocess.run([
        "docker", "exec", container_name, "mkdir", "-p", container_dir_path
    ])

    # Step 3: Copy the file to the container
    subprocess.run([
        "docker", "cp", local_file_path, f"{container_name}:{container_file_path}"
    ])

    # Optional: Execute any script inside the container
    # subprocess.run([
    #     "docker", "exec", container_name, "bash", "-c", "your_command_here"
    # ])

etl_csv()
load_file()