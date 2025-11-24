import pandas as pd

dataframe = pd.read_csv("hbahn/iperf_filtered.csv", sep=";")
# Print general information about the dataframe and its columns
dataframe.info()
#print(dataframe["timestamp_day"])
#print(dataframe["week_of_year"])
#print(dataframe["day_of_week"])

#print(dataframe["network"].dtype)
#print(dataframe["network"].nunique(dropna=False))
#print(dataframe["network"].isnull().sum())
#print(dataframe["network"].describe())
#print(dataframe["network"].value_counts(dropna=False).head(50))

print(dataframe["timestamp"].nunique())
print(dataframe["timestamp"].value_counts(dropna=False).head(50))
