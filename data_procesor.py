from pathlib import Path
import pandas as pd
import numpy as np
import os

TOPIC = "xxx"
BROKER = 'aa:1234'

# Kafka Skeleton for future work
def create_topic(topic_name, broker, num_partitions=1, replication_factor=1):
    """
    Ensure the Kafka topic exists; create it if it does not.
    """
    pass

def create_kafka_producer():
    """Creates a Kafka producer for sending raw packet data."""
    pass

def send_to_kafka(producer, topic, data):
    pass


def send_data():
    '''Sends data to kafka'''
    producer = create_kafka_producer()
    data = pd.read_csv("processed_data.csv", sep=';')

    for row in data.to_dict():
        send_to_kafka(producer,TOPIC,row)

    os.remove("processed_data.csv")

# ----------------------------------------

# Rudimentary logic, to be improved in the kafka message receival process
def process_latency_data(output_latency_path='hbahn/latency_filtered.csv'):
    """
    Read hbahn cell and latency CSVs, apply cleaning and filters, write filtered
    latency CSV (only rows for server 1.1.1.1 and valid min_latency) and return path.
    """
    # read with low_memory to avoid dtype warnings; read server_ip as str
    #df_cell = pd.read_csv('hbahn/cell_data.csv', sep=';', low_memory=False, dtype={'username': str})
    df_latency = pd.read_csv('hbahn/latency_data.csv', sep=';', low_memory=False, dtype={'username': str, 'server_ip': str})

    # normalize server_ip strings
    if 'server_ip' in df_latency.columns:
        df_latency['server_ip'] = df_latency['server_ip'].astype(str).str.strip()

    # Replace empty strings with NaN for numeric processing, but keep server_ip text
    for col in df_latency.columns:
        if col != 'server_ip' and df_latency[col].dtype == object:
            df_latency[col].replace('', np.nan, inplace=True)

    # For cell as well
    #for col in df_cell.columns:
    #    if df_cell[col].dtype == object:
    #        df_cell[col].replace('', np.nan, inplace=True)

    # Filter for 5G NSA only (if column exists)
    if 'network' in df_latency.columns:
        df_latency = df_latency[df_latency['network'].astype(str).str.strip() == '5G NSA'].copy()
    #if 'network' in df_cell.columns:
    #    df_cell = df_cell[df_cell['network'].astype(str).str.strip() == '5G NSA'].copy()

    # Ensure min_latency numeric
    if 'min_latency' in df_latency.columns:
        df_latency['min_latency'] = pd.to_numeric(df_latency['min_latency'], errors='coerce')

    # Filter latency for server '1.1.1.1'
    server_mask = df_latency['server_ip'] == '1.1.1.1' if 'server_ip' in df_latency.columns else pd.Series(False, index=df_latency.index)

    # Build min_latency mask (positive and below 99th percentile of positive values)
    if 'min_latency' in df_latency.columns:
        positive_mask = df_latency['min_latency'] > 0
        if positive_mask.any():
            upper = df_latency.loc[positive_mask, 'min_latency'].quantile(0.99)
            latency_mask = positive_mask & (df_latency['min_latency'] < upper)
        else:
            latency_mask = pd.Series(False, index=df_latency.index)
    else:
        latency_mask = pd.Series(True, index=df_latency.index)  # if no min_latency column, don't filter by it

    df_latency_filtered = df_latency[server_mask & latency_mask].copy()

    # write filtered latency CSV
    out_path = Path(output_latency_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_latency_filtered.to_csv(out_path, index=False, sep=';')

    return str(out_path)

def process_iperf_data(output_latency_path='hbahn/iperf_filtered.csv'):
    # read iperf dataset; keep username as string if present
    df_iperf = pd.read_csv('hbahn/iperf_data.csv', sep=';', low_memory=False, dtype={'username': str})

    # normalize string columns and replace empty strings with NaN
    for col in df_iperf.columns:
        if df_iperf[col].dtype == object:
            df_iperf[col] = df_iperf[col].astype(str).str.strip()
            df_iperf[col].replace({'': np.nan, 'nan': np.nan}, inplace=True)

    # coerce filesize to numeric if present
    if 'filesize' in df_iperf.columns:
        df_iperf['filesize'] = pd.to_numeric(df_iperf['filesize'], errors='coerce')

    # normalize network and protocol texts for robust comparison
    if 'network' in df_iperf.columns:
        df_iperf['network'] = df_iperf['network'].astype(str).str.strip()
    if 'protocol' in df_iperf.columns:
        df_iperf['protocol'] = df_iperf['protocol'].astype(str).str.strip().str.upper()

    # apply filters: network == '5G NSA', protocol == 'UDP', filesize == 10_000_000
    mask = pd.Series(True, index=df_iperf.index)
    if 'network' in df_iperf.columns:
        mask &= df_iperf['network'] == '5G NSA'
    # Commented for now, to produce a bigger sample number, but functional
    #if 'protocol' in df_iperf.columns:
    #    mask &= df_iperf['protocol'] == 'UDP'
    #if 'filesize' in df_iperf.columns:
    #    TEN_MB_BYTES = 10 * 1024 * 1024
    #    filesize_series = pd.to_numeric(df_iperf['filesize'], errors='coerce')
    #    mask &= (filesize_series.isin([10_000_000, TEN_MB_BYTES]) | filesize_series.between(TEN_MB_BYTES - 1000, TEN_MB_BYTES + 1000))

    df_iperf_filtered = df_iperf[mask].copy()

    # write filtered iperf CSV
    out_path = Path(output_latency_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_iperf_filtered.to_csv(out_path, index=False, sep=';')

    return str(out_path)


def add_static_locations(static_csv_path, location_file_path):
    """
    Read static CSV and location CSV (both ';' separated), merge on 'username'
    (left join), write a new CSV next to the original with suffix '_with_locations'
    and return the output path (string).
    """
    static_csv_path = Path(static_csv_path)
    sep = ';'

    # read the (small) locations file fully, coerce username to str and numeric fields
    loc = pd.read_csv(location_file_path, sep=sep, dtype={'username': str}, low_memory=False)
    loc_cols = [c for c in ('username', 'latitude', 'longitude', 'altitude') if c in loc.columns]
    loc = loc[loc_cols].copy()
    # coerce lat/lon/alt to numeric
    for c in ('latitude', 'longitude', 'altitude'):
        if c in loc.columns:
            loc[c] = pd.to_numeric(loc[c], errors='coerce')

    # prepare output path
    out_path = static_csv_path.with_name(static_csv_path.stem + "_with_locations" + static_csv_path.suffix)

    # if target CSV is large, stream it in chunks to avoid high memory usage
    try:
        filesize = static_csv_path.stat().st_size
    except Exception:
        filesize = 0

    CHUNK_THRESHOLD = 50 * 1024 * 1024  # 50 MB
    if filesize > CHUNK_THRESHOLD:
        # stream in chunks and append to output file
        chunk_iter = pd.read_csv(static_csv_path, sep=sep, dtype={'username': str}, chunksize=100000, low_memory=False)
        first = True
        for chunk in chunk_iter:
            # ensure username is string for join
            if 'username' in chunk.columns:
                chunk['username'] = chunk['username'].astype(str)
            merged = chunk.merge(loc, on='username', how='left')
            # coerce numeric columns if present
            for c in ('latitude', 'longitude', 'altitude'):
                if c in merged.columns:
                    merged[c] = pd.to_numeric(merged[c], errors='coerce')
            if first:
                merged.to_csv(out_path, index=False, sep=sep, mode='w')
                first = False
            else:
                merged.to_csv(out_path, index=False, sep=sep, mode='a', header=False)
    else:
        # small file: read fully
        target = pd.read_csv(static_csv_path, sep=sep, dtype={'username': str}, low_memory=False)
        merged = target.merge(loc, on='username', how='left')
        for c in ('latitude', 'longitude', 'altitude'):
            if c in merged.columns:
                merged[c] = pd.to_numeric(merged[c], errors='coerce')
        merged.to_csv(out_path, index=False, sep=sep)

    return str(out_path)


def merge_static_files(folder_path='static', location_file_path='static/static_locations.csv'):
    '''
    Merge static locations into cell_data.csv and latency_data.csv inside folder_path.
    Writes files with suffix '_with_locations.csv' and prints outputs.
    '''
    folder = Path(folder_path)
    for fname in ("cell_data.csv", "latency_data.csv"):
        src = folder / fname
        if not src.exists():
            print(f"{src} not found, skipping")
            continue
        out = add_static_locations(src, location_file_path)
        print(f"Wrote merged file: {out}")

def main():
    #merge_static_files(folder_path="static", location_file_path="static/static_locations.csv")
    #process_latency_data()
    process_iperf_data()

    # Processor logic
    #create_topic(TOPIC,BROKER)
    #process_data()
    #send_data()

if __name__ == "__main__":
    main()