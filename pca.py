import argparse
import os
import random
import s3fs
import joblib

import numpy as np

from sklearn.decomposition import PCA


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

    def write(self, uid, feature, rk):
        s3fs.S3FileSystem = S3FileSystemPatched
        fs = s3fs.S3FileSystem()

        with fs.open(self.output_path + 'pca.csv', mode='w') as resultfile:
            # data = [line.decode('utf8').strip() for line in data.tolist()]
            for i in range(len(uid)):
                line = "{},{},{},{},{}\n".format(uid[i], feature[i][0], feature[i][1], feature[i][2], rk[i])
                resultfile.write(line)


def get_data(path):
    print('get_data....')
    import pandas as pd
    s3fs.S3FileSystem = S3FileSystemPatched
    fs = s3fs.S3FileSystem()
    """get file list"""
    uid = []
    feature = []
    rk = []
    input_files = sorted([file for file in fs.ls(path) if file.find("part-") != -1])
    print(input_files)
    # input_files = ['part-00001.csv']
    index = 0

    for file in input_files:
        print(index)
        index += 1
        u_info = pd.read_csv("s3://" + file, header=None, usecols=[0]).values
        f_info = pd.read_csv("s3://" + file, header=None, usecols=[1]).values
        r_info = pd.read_csv("s3://" + file, header=None, usecols=[2]).values
        # u_info = pd.read_csv(file, header=None, usecols=[0]).values
        # f_info = pd.read_csv(file, header=None, usecols=[1]).values
        # r_info = pd.read_csv(file, header=None, usecols=[2]).values
        for i in range(u_info.shape[0]):
            uid.append(u_info[i][0])
            feature.append(np.array(f_info[i][0].split(' ')).astype('float32'))
            rk.append(r_info[i][0])
    return uid, feature, rk


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
    parser.add_argument(
        "--mode",
        type=str,
        default='train'
    )
    parser.add_argument(
        "--model_path",
        type=str,
        default=''
    )
    args, _ = parser.parse_known_args()
    input_data = args.data_input.split(',')[0]
    uid, feature, rk = get_data(input_data)
    feature = np.array(feature)
    if args.mode == 'train':
        pca = PCA(n_components=3)
        feature = pca.fit_transform(feature)
        joblib.dump(pca, 'pca.model')
        cmd = 's3cmd put ' + 'pca.model' + ' ' + \
              args.data_output + 'pca.model'
        os.system(cmd)
        print("best weights save!")
    else:
        cmd = 's3cmd get ' + args.model_path
        os.system(cmd)
        print('load model ok')
        pca = joblib.load('pca.model')
        feature = pca.transform(feature)

    writer = S3Filewrite(args)
    writer.write(uid, feature, rk)
