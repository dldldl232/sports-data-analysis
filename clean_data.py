import panda as pd

def clean_espn_data(input_file="collected_espn_data.csv", output_file="cleaned_espn_data.csv"):
    df = pd.read_csv(input_file)

    #cover data fields to dattime