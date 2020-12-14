import xmltodict
import pandas as pd
from tqdm import tqdm
from copy import deepcopy

file = open('./dataset/Posts.xml', 'r')
fetch_size = 100
year = '2020-07'
cols = [
    'Id',
    'PostTypeId',
    'AcceptedAnswerId',
    'ParentId',
    'CreationDate',
    'DeletionDate',
    'Score',
    'ViewCount',
    'Body',
    'OwnerUserId',
    'OwnerDisplayName',
    'LastEditorUserId',
    'LastEditorDisplayName',
    'LastEditDate',
    'LastActivityDate',
    'Title',
    'Tags',
    'AnswerCount',
    'CommentCount',
    'FavoriteCount',
    'ClosedDate',
    'CommunityOwnedDate',
    'ContentLicense'
]

print('Reading xml')

post_dicts = []
line = file.readline()
index = 0
while line:
    try:
        # print(line)
        data_dict = dict(xmltodict.parse(line[2:-1]))
        if data_dict['row']['@CreationDate'].startswith('{}'.format(year)):
            post_dicts.append(data_dict['row'])
        else:
            if len(post_dicts) > 0:
                break
        if index % 100000 == 0:
            print("{} records processed.   @{}".format(index, data_dict['row']['@CreationDate']))
    except Exception as e:
        print(e)
    finally:
        line = file.readline()
        index += 1
# print(post_dicts)

print('Converting to Dataframe')

df = pd.DataFrame(columns=cols)
for i, p_dict in tqdm(enumerate(post_dicts)):
        row_data = []
        for col_name in cols:
            if '@{}'.format(col_name) not in p_dict.keys():
                row_data.append(None)
            else:
                row_data.append(p_dict['@{}'.format(col_name)])
        df.loc[i] = row_data

# print(df)

print('Merging')

df2 = deepcopy(df)

merged = pd.merge(df, df2, left_on='Id', right_on='ParentId')

# print(merged)

print('Writting')

merged.to_csv('dataset/merged_{}.csv'.format(year), header=True, index=False)