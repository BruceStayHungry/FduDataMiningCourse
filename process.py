import pandas as pd
from tqdm import tqdm

year = 2010
path = "dataset/merged/merged_{}-07.csv".format(year)
df = pd.read_csv(path)

cur_question = 0
ac_answer = 0
q_t = ""
cur_answers = []
qa_data = {}
init = True

def process_time(t):
    # year = int(t[0:4])
    # month = int(t[5:7])
    day = int(t[8:10])
    hour = int(t[11:13])
    minute = int(t[14:16])
    second = int(t[17:19])
    msecond = int(t[20:])
    return [day, hour, minute, second, msecond]

def time_dif(q_t, a_t):
    _q_t = process_time(q_t)
    _a_t = process_time(a_t)
    t_dif = 0
    t_dif += (_a_t[0] - _q_t[0]) * 24 * 60 * 60 * 1000
    t_dif += (_a_t[1] - _q_t[1]) * 60 * 60 * 1000
    t_dif += (_a_t[2] - _q_t[2]) * 60 * 1000
    t_dif += (_a_t[3] - _q_t[3]) * 1000
    t_dif += (_a_t[4] - _q_t[4])
    return t_dif

def feature_from_title(title):
    return [0]

def feature_from_body(body):
    return [0]

def feature_from_tags(tags):
    return [0]

def feature_from_creation_time(creation_time):
    return [0]

for row in tqdm(df.itertuples()):
    # 处理Q的数据
    if getattr(row, 'Id_x') != cur_question:
        if init:
            init = False
        else:
            # 回答时间 === target
            min_time_diff = 999999999999
            max_score = -1
            for ans in cur_answers:
                if ans[0] == ac_answer:
                    min_time_diff = time_dif(q_t, ans[1])
                    break
                if ans[2] > max_score:
                    min_time_diff = time_dif(q_t, ans[1])
                    max_score = ans[2]
            
            if min_time_diff < 999999999999:
                qa_data[cur_question] = {}
                qa_data[cur_question]['ans_time'] = min_time_diff
                # question features
                qa_data[cur_question]['features'] = []
                qa_data[cur_question]['features'] += feature_from_creation_time(q_t)
                qa_data[cur_question]['features'] += feature_from_title(q_title)
                qa_data[cur_question]['features'] += feature_from_body(q_body)
                qa_data[cur_question]['features'] += feature_from_tags(q_tags)
            
        cur_question = getattr(row, 'Id_x')
        ac_answer = getattr(row, 'AcceptedAnswerId_x')
        q_t = getattr(row, 'CreationDate_x')
        q_title = getattr(row, 'Title_x')
        q_body = getattr(row, 'Body_x')
        q_tags = getattr(row, 'Tags_x')
        cur_answers = [[getattr(row, 'Id_y'), getattr(row, 'CreationDate_y'), getattr(row, 'Score_y')]]
    else:
        cur_answers.append([getattr(row, 'Id_y'), getattr(row, 'CreationDate_y'), getattr(row, 'Score_y')])

# 写出
o_path = 'dataset/target&features/target&features_{}.csv'.format(year)
with open(o_path, 'w') as f:
    for qid in qa_data:
        f.write('{},{}'.format(qid, qa_data[qid]['ans_time']))
        for feat in qa_data[qid]['features']:
            f.write(',{}'.format(feat))
        f.write('\n')

