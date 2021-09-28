restr = ''
ind = 0
import numpy as np
feas = ['src_gamefriend_battle_ratio', 'src_item_count', 'src_level', 'src_avg_mate_level', 'src_battlecnt', 'dst_gamefriend_battle_ratio', 'src_cnt_per_day', 'dst_level', 'dst_item_count', 'src_fr_ch_cnt', 'src_matchmodule2time', 'src_total_onlinetime', 'src_evening', 'dst_avg_mate_level', 'src_total_chat_cnt', 'src_matchmodule2cnt', 'src_matchmodule4time', 'src_matchmodule1time', 'src_matchmodule0time', 'src_miccnt', 'src_hour12', 'src_matchmodule0cnt', 'src_hour11', 'pprscore', 'src_matchmoduletime', 'src_hour13', 'src_wo_ch_cnt', 'src_matchmodule4cnt', 'src_hour21', 'dst_matchmoduletime', 'src_hour17', 'src_hour19', 'dst_evening', 'dst_matchmodule2time', 'src_hour14', 'src_morning', 'src_hour15', 'src_hour20', 'dst_total_onlinetime', 'src_hour22', 'src_hour18', 'src_hour23', 'src_hour16', 'src_noon', 'numcommonfriends', 'playerduration', 'dst_battlecnt', 'dst_fr_ch_cnt', 'src_allmatchcnt', 'intimacyval']

o_f = []
while True:
    inp = input()
    if inp == '-1':
        break
    o_f.append(inp)
fea_index=[]
for fea in feas:
    fea_index.append(o_f.index(fea))
fea_index.sort()
print(fea_index)
print('123')

# fea, scores = np.array(fea), np.array(scores)
# print(fea[scores.argsort()])