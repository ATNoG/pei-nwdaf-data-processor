import pandas as pd

dataframe = pd.read_csv("mobile/cell_data.csv", sep=";")
# Print general information about the dataframe and its columns
dataframe.info()
