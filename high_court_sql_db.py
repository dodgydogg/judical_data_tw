import glob
import sqlite3
import pandas as pd
from tqdm import tqdm


conn = sqlite3.connect('high_court_cases.db')
cur = conn.cursor()

with open('high_court_schema.sql', encoding='utf-8') as f:
    cur.executescript(f.read())


# load txt files
txt_file_list = glob.glob('raw_txt/**/*HM.刑事訴訟.txt', recursive=True)


def insert_case(case_info):
    # SQL query to insert data into the Cases table
    sql = """
    INSERT INTO Cases (
        c1控制碼, c2法院, c3案號_年, c4案號_字別, c5案號_號,
        c6案由, c7法官名1, c8法官名2, c9法官名3, c10原審法院,
        c11原審案號_年, c12原審案號_字別, c13原審案號_號,
        c14全案終結日期_年, c15全案終結日期_月, c16全案終結日期_日,
        c17全案終結情形, c18得上訴, c19不得上訴, c20得抗告,
        c21不得抗告, c22自訴人是否有律師代理,
        c23是否因刑訴法第361條駁回, c24裁判內容出現_消除對婦女一切形式歧視公約,
        c25裁判內容出現_兒童權利公約, c26裁判內容出現_兩人權公約,
        c27裁判內容出現_身心障礙者權利公約,
        c28犯選罷法終結選舉類別,
        c29裁判書ID
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    # Execute the query with the provided case_info list
    cur.execute(sql, case_info)

def insert_defendant(defendant_info):
    sql = """
    INSERT INTO Defendants (
    d1控制碼, d2被告終結日期_年, d3被告終結日期_月, d4被告終結日期_日, 
    d5被告終結情形, d6辯護及代理, d7保安處分_感化教育, d8保安處分_監護, 
    d9保安處分_禁戒, d10保安處分_強制工作, d11保安處分_保護管束, d12保安處分_強制治療, 
    d13保安處分_驅逐出境, d14保安處分_安置輔導, d15保安處分_其他, d16裁判程序,
    d17宣告沒收適用法律_刑法第38條第1項_違禁物, d18宣告沒收適用法律_刑法第38條第2項_犯罪工具產物, 
    d19宣告沒收適用法律_刑法第38條第3項_犯罪工具產物,
    d20宣告沒收適用法律_刑法第38_1條第1項_犯罪所得, 
    d21宣告沒收適用法律_刑法第38_1條第2項_犯罪所得, d22宣告沒收適用法律_刑法第38_1條第2項第1款_犯罪所得,
    d23宣告沒收適用法律_刑法第38_1條第2項第2款_犯罪所得, d24宣告沒收適用法律_刑法第38_1條第2項第3款_犯罪所得,
    d25沒收資料_沒收客體, d26沒收資料_沒收金額,
    d27是否宣告緩刑,
    d28宣告緩刑附記應遵收事項_向被害人道歉, d29宣告緩刑附記應遵收事項_立悔過書,
    d30宣告緩刑附記應遵收事項_向被害人支付相當數額之財產或非財產上之損害賠償,
    d31向被害人支付損害賠償金額, d32宣告緩刑附記應遵收事項_向公庫支付一定之金額,
    d33向公庫支付金額, d34宣告緩刑附記應遵收事項_向指定之政府機關構行政法人社區公益機構或團體提供40小時以上240小時以下之義務勞務,
    d35宣告緩刑附記應遵收事項_完成戒癮治療精神治療心理輔導或其他適當之處遇措施,
    d36宣告緩刑附記應遵收事項_保護被害人安全之必要命令,
    d37宣告緩刑附記應遵收事項_預防再犯所為之必要命令,
    d38違反家暴法緩刑期間應遵守事項_第38條第2項第1款禁止實施家庭暴力,
    d39違反家暴法緩刑期間應遵守事項_第38條第2項第3款強制遷出,
    d40違反家暴法緩刑期間應遵守事項第38條第2項第2款禁止騷擾等行為,
    d41違反家暴法緩刑期間應遵守事項第38條第2項第5款強制戒癮治療,
    d42違反家暴法緩刑期間應遵守事項第38條第2項第5款強制精神治療,
    d43違反家暴法緩刑期間應遵守事項第38條第2項第5款強制心理輔導,
    d44違反家暴法緩刑期間應遵守事項第38條第2項第5款強制其他治療輔導,
    d45違反家暴法緩刑期間應遵守事項第38條第2項第6款其他必要保護被害人等措施,
    d46違反家暴法緩刑期間應遵守事項第38條第2項第4款強制遠離,
    d47違反家暴法緩刑期間應遵守事項第38條第2項第5款強制認知教育輔導,
    d48違反家暴法緩刑期間應遵守事項第38條第2項第5款強制親職教育輔導,
    d49違反家暴法緩刑期間應遵守事項第38條第2項第5款舊法其他更生保護事項,
    d50減刑條例年度1, d51減刑條例年度2, d52減刑條例年度3, d53減刑條例年度4,
    d54應執行刑1020401前_主刑, d55應執行刑1020401前_有期徒刑,
    d56定應執行刑_有期徒刑得易科, d57定應執行刑_有期徒刑不得易科逾6月,
    d58定應執行刑_有期徒刑不得易科6月以下, d59定應執行刑_拘役,
    d60定應執行刑_罰金類別, d61定應執行刑_罰金, d62定應執行刑_無期徒刑或死刑,
    d63是否為累犯, d64是否有刑法第91條之1第1項審前鑑定,
    d65上訴人別1, d66上訴人別2, d67上訴人別3,
    c29裁判書ID,
    Case_ID
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
    cur.execute(sql, defendant_info)

def insert_charge(charge_info):
    sql="""
    INSERT INTO charges (
    ch1控制碼,
    ch2刑事法令,
    ch3刑事法令_條,
    ch4刑事法令_之條,
    ch5刑事法令_條_段,
    ch6刑事法令_項,
    ch7刑事法令_項_段,
    ch8刑事法令_款,
    ch9修法年月,
    ch10被告罪名裁判結果,
    ch11宣告有期徒刑,
    ch12拘役日數,
    ch13罰金金額,
    ch14是否得易科,
    ch15是否褫奪公權,
    ch16罪犯類型_少年犯,
    ch17罪犯類型_幫助犯,
    ch18罪犯類型_未遂犯,
    ch19罪犯類型_家庭暴力,
    Defendant_ID
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cur.execute(sql, charge_info)
    
def insert_aggravation(aggravation_info):
    sql="""
    INSERT INTO Aggravation (a1控制碼, a2量刑加重, a3, a4, a5, a6, a7, Charge_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    cur.execute(sql, aggravation_info)
    

def insert_commutation(commutation_info):
    sql="""
    INSERT INTO Commutation (r1控制碼, r2量刑減輕, r3, r4, r5, r6, r7, Charge_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    cur.execute(sql, commutation_info)

def insert_s_aggravation(s_aggravation_info):
    sql="""
    INSERT INTO SpecialAggravation (sa1控制碼, sa2量刑特殊加重, Charge_ID) VALUES (?, ?, ?)
    """
    cur.execute(sql, s_aggravation_info)

def insert_s_commutation(s_commutation_info):
    sql="""
    INSERT INTO SpecialCommutation (sr1控制碼, sr2量刑特殊減輕, Charge_ID) VALUES (?, ?, ?)
    """
    cur.execute(sql, s_commutation_info)

def insert_tf(tf_info):
    sql="""
    INSERT INTO ThirdPersonForfeiture (
    t1控制碼,
    t2沒收程序裁判結果,
    t3宣告沒收適用法律_刑法第38條第1項_違禁物,
    t4宣告沒收適用法律_刑法第38條第3項_犯罪工具產物,
    t5宣告沒收適用法律_刑法第38_1條第2項_犯罪所得,
    t6宣告沒收適用法律_刑法第38_1條第2項第1款_犯罪所得,
    t7宣告沒收適用法律_刑法第38_1條第2項第2款_犯罪所得,
    t8宣告沒收適用法律_刑法第38_1條第2項第3款_犯罪所得,
    t9沒收資料_沒收客體,
    t10沒收資料_沒收金額,
    c29裁判書ID,
    Case_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cur.execute(sql, tf_info)

def insert_tf_law(tf_law_infro):
    sql="""
    INSERT INTO ThirdPersonForfeitureLaw (
    tl1控制碼,
	tl2刑事法令,
	tl3刑事法令_條,
	tl4刑事法令_之條,
	tl5刑事法令_條_段,
	tl6刑事法令_項,
	tl7刑事法令_項_段,
	tl8刑事法令_款,
	TPF_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cur.execute(sql, tf_law_infro)

for file in tqdm(txt_file_list): # loop through all txt files
    with open(file ,'r', encoding='utf-8') as f:
        txt_lines = f.read() # read txt file into string
        case_list = txt_lines.split('#') # split string into list of cases
        for case in case_list: # loop through all cases
            case_lines = case.split('\n')
            for line in case_lines: # loop through all lines in a case
                columns = line.split('!')
                if columns[0] == '0':
                    case_info = columns
                    case_info = case_info[:-1]
                    verdict_id = case_info[28]
                    # cur.execute("BEGIN TRANSACTION;")
                    insert_case(case_info)
                    case_id = cur.lastrowid
                elif columns[0] == '1':
                    defendant_info = columns
                    defendant_info = defendant_info[:-1]
                    defendant_info.append(verdict_id)
                    defendant_info.append(case_id)
                    insert_defendant(defendant_info)
                    defendant_id = cur.lastrowid
                elif columns[0] == '1.1':
                    charge_info = columns
                    charge_info = charge_info[:-1]
                    charge_info.append(defendant_id)
                    insert_charge(charge_info)
                    charge_id = cur.lastrowid
                elif columns[0] == '1.1.1':
                    aggravation_info = columns
                    while len(aggravation_info) < 7:
                        aggravation_info.append('NULL')
                    aggravation_info.append(charge_id)
                    insert_aggravation(aggravation_info)
                elif columns[0] == '1.1.2':
                    commutation_info = columns
                    while len(commutation_info) < 7:
                        commutation_info.append('NULL')
                    commutation_info.append(charge_id)
                    insert_commutation(commutation_info)
                elif columns[0] == '1.1.3':
                    s_aggravation_info = columns
                    s_aggravation_info = s_aggravation_info[:-1]
                    s_aggravation_info.append(charge_id)
                    insert_s_aggravation(s_aggravation_info)
                elif columns[0] == '1.1.4':
                    s_commutation_info = columns
                    s_commutation_info = s_commutation_info[:-1]
                    s_commutation_info.append(charge_id)
                    insert_s_commutation(s_commutation_info)
                elif columns[0] == '2':
                    tf_info = columns
                    tf_info = tf_info[:-1]
                    tf_info.append(verdict_id)
                    tf_info.append(case_id)
                    insert_tf(tf_info)
                    tf_id = cur.lastrowid
                elif columns[0] == '2.1':
                    tf_law_info = columns
                    tf_law_info = tf_law_info[:-1]
                    tf_law_info.append(tf_id)
                    insert_tf_law(tf_law_info)
        conn.commit()