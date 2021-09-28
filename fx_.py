import argparse
import os
import s3fs
import time
# from chim import ChiMerge
from process import S3FileSystemPatched


def get_data(path, file_idx, no_image=False):
    import pandas as pd
    # import numpy as np
    s3fs.S3FileSystem = S3FileSystemPatched
    fs = s3fs.S3FileSystem()
    """get file list"""
    # input_files = ['part-00002.csv']
    input_files = sorted([file for file in fs.ls(path) if file.find("part-") != -1])
    # input_files = input_files[:10]
    if file_idx is not None:
        input_files = [input_files[file_idx]]

    u = []
    p = []
    index = 0
    for file in input_files:
        print(index)
        index += 1
        p_info = pd.read_csv("s3://" + file, header=None, usecols=[i for i in range(2)]).values
        u_info = pd.read_csv("s3://" + file, header=None, usecols=[i for i in range(2, 153)]).values
        for i in range(u_info.shape[0]):
            p.append((p_info[i]))
            u.append((u_info[i].astype('float32')))
    return p, u


import numpy as np
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument(
    "--data_input",
    type=str,
)
parser.add_argument(
    "--data_output",
    type=str
)
args, _ = parser.parse_known_args()
input_data = args.data_input.split(',')[0]
p, df = get_data(input_data, None, no_image=True)
output = args.data_output
df = pd.DataFrame(df)
p = pd.DataFrame(p)
# train_df = df  # .sample(n=len(df) // 10, random_state=2020)
print('load ok')
lenss = []
for i in range(1, 151):
    print(i)
    df[i], bins = pd.qcut(df[i], 15, labels=False, duplicates='drop', retbins=True)
    print(bins)
    lenss.append(df[i].max()+1)
print(lenss)
all = pd.concat([p, df], 1)
block_size = 100000
n = len(all) // block_size
for i in range(n):
    all[i * block_size:(i + 1) * block_size].to_csv(output+'part_' + str(i) + '.csv', sep=',', index=False)
all[n * block_size:].to_csv(output+'part_' + str(n) + '.csv', sep=',', index=False)
