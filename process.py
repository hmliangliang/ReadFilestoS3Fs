import argparse
import os
import random
import s3fs
#s3fs库文档https://s3fs.readthedocs.io/en/latest/

import numpy as np


class S3FileSystemPatched(s3fs.S3FileSystem):
    def __init__(self, *k, **kw):
        super(S3FileSystemPatched, self).__init__(*k,
                                                  key=os.environ['AWS_ACCESS_KEY_ID'],
                                                  secret=os.environ['AWS_SECRET_ACCESS_KEY'],
                                                  client_kwargs={'endpoint_url': 'http://' + os.environ['S3_ENDPOINT']},
                                                  **kw
                                                  )


class S3Filewrite:
    def __init__(self, args):
        super(S3Filewrite, self).__init__()
        self.output_path = args.data_output

    def write(self, id_dict):
        s3fs.S3FileSystem = S3FileSystemPatched
        fs = s3fs.S3FileSystem()

        with fs.open(self.output_path + 'id_dict.csv', mode='w') as resultfile:
            # data = [line.decode('utf8').strip() for line in data.tolist()]
            for key in id_dict.keys():
                line = ' '.join(str(i) for i in id_dict[key])
                line = str(key) + ',' + str(line) + '\n'
                resultfile.write(line)


def get_data(path):
    print('get_data....')
    import pandas as pd
    s3fs.S3FileSystem = S3FileSystemPatched
    fs = s3fs.S3FileSystem()
    """get file list"""
    src = []
    input_files = sorted([file for file in fs.ls(path) if file.find("part-") != -1])
    index = 0
    for file in input_files:
        print(index)
        index += 1
        p_info = pd.read_csv("s3://" + file, header=None, usecols=[i for i in range(3)]).values
        for i in range(p_info.shape[0]):
            src.append(str(p_info[i][0]))
    print(len(src))
    return list(set(src))


def mapping(id_list):
    print('mapping.....')
    s = len(id_list)
    print('num: ' + str(s))
    random.shuffle(id_list)
    random_data = np.random.randn(s, 128)
    id_dict = {}
    for i in range(s):
        id_dict[id_list[i]] = random_data[i]
    return id_dict


def write(id_dict, args):
    print('writing....')
    writer = S3Filewrite(args)
    writer.write(id_dict)


if __name__ == '__main__':
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
    id_list = get_data(input_data)
    id_dict = mapping(id_list)
    write(id_dict, args)
