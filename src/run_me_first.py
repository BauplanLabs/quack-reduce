

def donwload_data(url: str, target_file: str):
    """
    Download a file from a url and save it to a target file
    """
    import requests
    
    r = requests.get(url)
    open(target_file, 'wb').write(r.content)

    return True


def download_taxi_data():
    """
    Download the taxi data from the duckdb repo - if the file disappears, 
    you can of course replace it with any other version of the same dataset
    """

    url = 'https://github.com/cwida/duckdb-data/releases/download/v1.0/taxi_2019_04.parquet'
    file_name = 'data/taxi_2019_04.parquet'
    donwload_data(url, file_name)

    return file_name


def setup_project():
    # first download the data
    download_taxi_data()

    # all done
    print("All done! See you, duck cowboy!")
    return     


if __name__ == "__main__":
    setup_project()