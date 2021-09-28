restr = ''
ind = 0
while True:
    inp = input()
    if 'HIT' in inp or 'NDCG' in inp:
        ind += 1
        restr += inp.split('@')[-1].split(' ')[-1]+'	'
        if ind >= 8:
            break
print()
print(restr)

