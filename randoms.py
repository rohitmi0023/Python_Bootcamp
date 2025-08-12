# %%
lists = ['apple', 'mango']
joined = ['_'.join(ele) for ele in lists]
print(joined)

lists = [['apple', 'banana'], ['mango']]
joined = ['_'.join(ele) for ele in lists]
print(joined)