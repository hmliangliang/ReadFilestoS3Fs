# import sparkfuel as sf

# from pyspark.sql.session import SparkSession
# from pyspark.mllib.util import MLUtils
# import pyspark
import pandas as pd
# from pytoolkit import TDWSQLProvider
# from pytoolkit import TableDesc
# from pytoolkit import TableInfo
# from pytoolkit import TDWUtil
# import scipy.sparse as sp
import numpy as np
from collections import namedtuple
from utils.s3utils import *
import s3fs
import time

Data = namedtuple('Data', ['x', 'adjacency_dict', 'train_index'])


def _map_to_pandas(rdds):
    return [pd.DataFrame(list(rdds))]


def topandas(df, n_partitions=None):
    """分布式DataFrame"""
    if n_partitions is not None:
        df = df.repartition(n_partitions)
    df_pand = df.rdd.mapPartitions(_map_to_pandas).collect()
    df_pand = pd.concat(df_pand)
    df_pand.columns = df.columns

    return df_pand


class PortraitFeatureData():

    def __init__(self):
        self.src_id = None
        self.dst_id = None

    def get_file_num(self, args):
        path = args.data_input.split(',')[0]
        s3fs.S3FileSystem = S3FileSystemPatched
        fs = s3fs.S3FileSystem()
        input_files = sorted([file for file in fs.ls(path) if file.find("part-") != -1])
        return len(input_files)

    def loadS3Data(self, path, file_idx=None):
        print("Loading cmelive dataset from S3......")

        s3fs.S3FileSystem = S3FileSystemPatched
        fs = s3fs.S3FileSystem()

        """get file list"""
        input_files = sorted([file for file in fs.ls(path) if file.find("part-") != -1])

        if file_idx is not None:
            input_files = [input_files[file_idx]]

        """data"""
        df = (pd.read_csv("s3://" + file, sep=';', header=None) for file in input_files)

        df = pd.concat(df)
        df = df[1:]
        df = df[0].str.split(',', expand=True)

        print("df.show(): ", df.head(5))
        print("data length:", len(df))
        print("data rows, data cols,:", print(df.shape[0], df.shape[1]))

        return np.apply_along_axis(self.preprocess, 1, df.values)

    def preprocess(self, data):
        src_id = data[0]
        dst_id = data[1]
        label = data[2]
        interact_feature_x1 = np.expand_dims(np.array(data[3:78], dtype=np.float32), 0)
        interact_feature_x2 = np.expand_dims(np.array(data[78:153], dtype=np.float32), 0)
        pic_feature_x1 = np.expand_dims(np.array(data[-2].split(' '), dtype=np.float32), 0)
        pic_feature_x2 = np.expand_dims(np.array(data[-1].split(' '), dtype=np.float32), 0)
        return np.float32(label), interact_feature_x1, interact_feature_x2, pic_feature_x1, pic_feature_x2, np.long(
            src_id), np.long(dst_id)

    def get_train_test_index(self, data_size):
        np.random.seed(2021)
        random_list = np.random.permutation(data_size)
        return random_list[:int(data_size * 0.8)], random_list[int(data_size * 0.8):]

    def get_src_dst_id(self):
        return self.src_id, self.dst_id

    def process_data(self, args, file_idx=None):
        print("Process data ...")

        # input1, input2 = args.data_input.split(',')
        input_ = args.data_input.split(',')[0]
        data = self.loadS3Data(input_, file_idx)
        if args.mode == "train":
            np.random.shuffle(data)
        label, internact_fea_x1, internact_fea_x2, pic_fea_x1, pic_fea_x2 = np.array(np.expand_dims(data[:, 0], axis=1),
                                                                                     dtype=np.float32), \
                                                                            np.concatenate(data[:, 1], axis=0), \
                                                                            np.concatenate(data[:, 2], axis=0), \
                                                                            np.concatenate(data[:, 3], axis=0), \
                                                                            np.concatenate(data[:, 4], axis=0)
        self.src_id = list(data[:, 5])
        self.dst_id = list(data[:, 6])

        return label, self.normalize_feature(internact_fea_x1), \
               self.normalize_feature(internact_fea_x2), \
               pic_fea_x1, \
               pic_fea_x2, self.src_id, self.dst_id

    def normalize_feature(self, features):
        normal_features = features / (features.max(0) + 0.0000001)

        return normal_features

    def data(self):
        """return Data struct，including features, adjacency, train_index"""
        return self._data
