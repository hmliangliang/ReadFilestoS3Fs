import s3fs
import numpy as np
from utils.s3utils import S3FileSystemPatched


def get_data(path, file_idx, no_image=False, xg=False):
    import pandas as pd
    s3fs.S3FileSystem = S3FileSystemPatched
    fs = s3fs.S3FileSystem()
    """get file list"""
    input_files = sorted([file for file in fs.ls(path) if file.find("part-") != -1])
    if file_idx is not None:
        input_files = [input_files[file_idx]]

    max1 = [7.0, 7.0, 7.0, 7.0, 7.0, 7.0, 7.0, 6.9877777099609375, 7.0, 7.0, 7.0, 7.0, 7.0, 7.0, 7.0, 7.0, 7.0,
            7.0,
            7.356944561004639, 8.0, 8.0, 8.0, 8.0, 7.920000076293945, 135.34083557128906, 194.0, 2.0, 862.0,
            150.0,
            1.0000000116860974e-07, 1.0000000116860974e-07, 1.0000000116860974e-07, 6341.0, 3965.0, 1469.0,
            271.0,
            1.0000000116860974e-07, 1172.0, 463.0, 6307.0, 1.0000000116860974e-07, 1.0000000116860974e-07,
            91148.0,
            50900.0, 69008.0, 25030.0, 172397.0, 401684.0, 393394.0, 156298.0, 320481.0,
            1.0000000116860974e-07,
            298341.0,
            1272.0, 332.0, 604.0, 1.0000000116860974e-07, 642.0, 1328.0, 13059.0, 150.0,
            1.0000000116860974e-07,
            1.0000001192092896, 7.0, 7.0, 7.0, 7.0, 7.0, 7.0, 7.0, 7.0, 7.0, 7.0, 7.0, 7.0]
    max2 = [7.0, 7.0, 7.0, 7.0, 7.0, 7.0, 7.0, 7.0, 8.0, 7.94944429397583, 8.0, 7.573611259460449,
            131.5399932861328,
            138.0, 7.0, 862.0, 150.0, 7.0, 7.356944561004639, 8.0, 4105.0, 1374.0, 1282.0, 271.0,
            135.34083557128906,
            898.0, 234.0, 4048.0, 150.0, 1.0000000116860974e-07, 75138.0, 34140.0, 71776.0, 25030.0, 176262.0,
            375601.0,
            375601.0, 116946.0, 252806.0, 6307.0, 273885.0, 906.0, 91148.0, 50900.0, 69008.0, 25030.0,
            172397.0, 401684.0,
            393394.0, 156298.0, 320481.0, 16.0, 298341.0, 2905.0, 332.0, 19377.0, 19383.0, 642.0, 1328.0,
            13059.0, 150.0,
            17.0, 5.0, 7.0, 7.0, 39.804443359375, 30.538333892822266, 36.62361145019531, 47.04777908325195,
            38.55638885498047, 30.453887939453125, 38.858890533447266, 45.641109466552734, 301763.0, 375601.0]
    if xg:
        im_fea = [13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 27, 28, 31, 35, 36, 37, 42, 49, 51, 52, 53, 55,
                  56, 58, 60, 61, 62, 63, 65, 90, 94, 100, 112, 116, 125, 126, 128, 129, 130, 133, 142, 144, 145, 146,
                  150, 151, 152]
        max_ = np.array(max1 + max2)[np.array(im_fea)-3].tolist()
        max1 = max_[:len(im_fea) // 2]
        max2 = max_[len(im_fea) // 2:]

    u1 = []
    u2 = []
    i1 = []
    i2 = []
    src = []
    dst = []
    label = []
    index = 0
    if no_image:
        for file in input_files:
            print(index)
            index += 1
            p_info = pd.read_csv("s3://" + file, header=None, usecols=[i for i in range(3)]).values
            if xg:
                u_info = pd.read_csv("s3://" + file, header=None, usecols=[i for i in im_fea]).values
                spilit_num = len(im_fea) // 2
            else:
                u_info = pd.read_csv("s3://" + file, header=None, usecols=[i for i in range(3, 153)]).values
                spilit_num = 75
            for i in range(u_info.shape[0]):
                label.append(p_info[i][2])
                src.append(str(p_info[i][0]))
                dst.append(str(p_info[i][1]))
                u1.append((u_info[i][0:spilit_num].astype('float32') / max1).astype('float32'))
                u2.append((u_info[i][spilit_num:spilit_num * 2].astype('float32') / max2).astype('float32'))
        return np.array(u1), np.array(u2), np.array(src), np.array(dst), np.array(label)

    else:
        for file in input_files:
            print(index)
            index += 1
            p_info = pd.read_csv("s3://" + file, header=None, usecols=[i for i in range(3)]).values
            if xg:
                u_info = pd.read_csv("s3://" + file, header=None, usecols=[i for i in im_fea]).values
                spilit_num = len(im_fea) // 2
            else:
                u_info = pd.read_csv("s3://" + file, header=None, usecols=[i for i in range(3, 153)]).values
                spilit_num = 75
            i_info = pd.read_csv("s3://" + file, header=None, usecols=[i for i in range(153, 155)]).values
            for i in range(u_info.shape[0]):
                label.append(p_info[i][2])
                src.append(str(p_info[i][0]))
                dst.append(str(p_info[i][1]))
                u1.append((u_info[i][0:spilit_num].astype('float32') / max1).astype('float32'))
                u2.append((u_info[i][spilit_num:spilit_num * 2].astype('float32') / max2).astype('float32'))
                i1.append(np.array(i_info[i][0].split(' ')).astype('float32'))
                i2.append(np.array(i_info[i][1].split(' ')).astype('float32'))
        return np.array(u1), np.array(u2), np.array(i1), np.array(i2), np.array(src), np.array(dst), np.array(label)

