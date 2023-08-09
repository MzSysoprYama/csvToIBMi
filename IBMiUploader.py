# -*- coding: UTF-8 -*-
import os
import pandas as pd
from chardet import detect
import urllib
import re
from sqlalchemy import create_engine
import common
from sqlalchemy.exc import SQLAlchemyError
import numpy as np
import logging
import datetime
from flask import request

# トランザクションを使用していない場合はODBCのコミットモードを"即時コミット(*NONE)" に変更する
# ODBCデータソース名とIBMiのユーザー名とパスワード
DATA_SOURCE_NAME = 'MTZAPI'
# 使用しているユーザー名、パスワードを入力して下さい。
UID = 'MZ'
PID = 'MZ'

# pandasでNANになってしまったときに代わりに代入する値
IMPUTATION_VALUE_FLOAT_INT = 0
IMPUTATION_VALUE_STRING = "NULL"
# 列名がうまくアップできない場合にデフォルトで付ける名前
DEFAULT_COLUMN_NAME = "COL_"

# 一時ファイルの名前用に使う日付
t_delta = datetime.timedelta(hours=9)
JST = datetime.timezone(t_delta, 'JST')
now = datetime.datetime.now(JST)

# logの設定
logger = logging.getLogger(__name__)
formatter = '%(asctime)s:%(name)s:%(levelname)s:%(message)s'

# logger
logging.basicConfig(
    filename='./IBMiCSVUploader_logger.log',
    level=logging.INFO,
    format=formatter
)

# INFOに設定
logger.setLevel(logging.INFO)

# pandasデータフレームからSQLのインサート文を生成する。
def df_to_sql_bulk_insert(df: pd.DataFrame, table: str, **kwargs)-> tuple:
    df = df.copy().assign(**kwargs)
   
    # 欠損値が1以上あれば、列の型ごとに欠損値を埋める
    if df.isnull().values.sum() > 0: #欠損があれば
        df_true_false = df.isnull().any()
        # 型のシリーズ
        sr = df.dtypes
        df_concat = pd.concat([sr, df_true_false], axis=1)
      
        # Trueのみ抽出
        extract = df_concat[df_concat[1]== True]
        
        # 文字列なら"NULL"をそうでなければ0を入れるが、欠損ありの数値はfloatになってしまうので0.0になる
        for i in range(len(extract)):
            print("列"+str(extract.iloc[i,0]))
            if extract.iloc[i,0] != object:
                col_name = extract.index[extract.iloc[:, 0] == extract.iloc[i, 0]]
                print(col_name.values[0])
                df[col_name] = df[col_name].fillna(IMPUTATION_VALUE_FLOAT_INT)
            else:
                col_name = extract.index[extract.iloc[:, 0] == extract.iloc[i, 0]]
                df[col_name] = df[col_name].fillna(IMPUTATION_VALUE_STRING)
        


    # ヘッダーに全角があったらDEFAULT_COLUMN_NAMEに、半角で7文字以上だったら削る
    cnt = 1
    for v in df.columns:
        if common.isalnum_ascii_re(v) and len(v) > 6:
            df = df.rename(columns={v: v[:6]})
        elif not common.isalnum_ascii_re(v):
            zero_pad = f"{cnt:02}" if len(str(cnt)) < 2  else str(cnt)
            df = df.rename(columns={v: DEFAULT_COLUMN_NAME + zero_pad})
        cnt += 1 

    columns = ", ".join(df.columns)
 
    # 文字列にする全部
    tuples = map(str, df.itertuples(index=False, name=None))
    
  
    # 不要な文字を変換、小文字は大文字に
    values = re.sub(r"(nan|Nan)", "NULL", (",\n" + " " * 7).join(tuples))
    values = re.sub('(u3000)', '　', values)
    values = re.sub(r'\\', '', values)
    values = values.upper()

    # INSERT文を生成して返す
    return f"INSERT INTO {table} ({columns})\nVALUES {values}", df


# pandasデータフレームからSQLのCREATE文を生成する。
def df_to_sql_create_table(df: pd.DataFrame, table: str, column_types: str)-> str:
    lngt=[]
    # 型のシリーズ
    sr = df.dtypes
    type_list = column_types.split(',')

    df_type = pd.DataFrame(sr)

    # CSVの値の最大値を取得してcreate文の桁数を決める。
    for i in range(len(df.columns)):
        if sr[i] == object:
            # 欠損値を0で補完したのでastypeで0をキャストしておく
            l = max(map(common.ibmi_str_count, df.iloc[:,i].astype(str)))
            lngt.append(f"VARCHAR({l})")
        elif sr[i] == np.int64:
            if type_list[i] == 'string':
                l = max(map(common.ibmi_str_count, df.iloc[:,i].astype(str)))
                lngt.append(f"VARCHAR({l})")
            else:
                lngt.append(common.int_str(df.iloc[:,i]))   
        elif sr[i] == np.float64:
            if type_list[i] == 'string':
                l = max(map(common.ibmi_str_count, df.iloc[:,i].astype(str)))
                lngt.append(f"VARCHAR({l})")
            else:
                lngt.append(common.decimal_str(df.iloc[:,i])) 

    df_type.insert(1, 1, lngt)
    df_type.iloc[:,0]= df_type.iloc[:,0].astype(str)
    sqlStr = ",\n".join(list(map(lambda x,y: " "*3 + x + " "*3 + y , df_type.index.values, df_type.iloc[:,1])))

    # create文を生成して返す
    return f"CREATE TABLE {table}\n(\n{sqlStr}\n)"

# drop
def drop_sql(table: str)->str:
    return f"DROP TABLE {table}"

# 既にテーブルがあるかチェック
def check_exist_tabele_sql(table: str)->str:
    strs = table.split('.')
    # print(strs)
    return f"SELECT TABLE_NAME FROM SYSIBM.TABLES WHERE TABLE_SCHEMA = '{strs[0]}' AND TABLE_NAME = '{strs[1]}'"


# IBMiにupload
def Upload(file, table_name, start_row, column_types):
    err_flg = False

    # 現在時刻
    d = now.strftime('%Y%m%d%H%M%S')
  
    try:
        # csvを一時ファイルとして保存
        temp_file = f'temp{d}.csv'
        file.save(temp_file)

        # バイナリでリード
        with open(temp_file, 'rb') as f:  
            b = f.read()
        # エンコード確認
        enc = detect(b)

        # 文字列001が1になるので文字列か判断しintにならないように
        split_c_types = column_types.split(',')
        # 文字列のカラムが何列目にあるかを確認
        string_column_index_with_none = list(map(lambda z:  z[0] if z[1] == 'string' else None ,enumerate(split_c_types)))
        # 文字列のカラムのindexだけ格納
        string_column_index = list(filter(lambda a:a is not None, string_column_index_with_none))
        # {0: 'str'}の様なdictに変換する
        dict_dtypes = {x: 'str' for x in string_column_index}
        # 正しい型で読み込む
        df = pd.read_csv(temp_file, encoding=enc['encoding'], skiprows=start_row - 1, dtype=dict_dtypes)
        # 一時ファイル削除
        os.remove(temp_file)

    except Exception as e:
        logger.error({
            'action': 'upload',
            'message': 'FILE OPEN error = %s' % e
        })
        err_flg = True
        
    else:
        pass

    if err_flg == False:
        
        logger.info("===START UPLOAD===")

        insert_sql, df_imputation =(df_to_sql_bulk_insert(df, table_name))

        cretae_sql=(df_to_sql_create_table(df_imputation, table_name, column_types))
        
        # odbc接続準備 %等があった場合のためにエンコード
        quoted = urllib.parse.quote_plus(f"DSN={DATA_SOURCE_NAME};uid={UID};pwd={PID}")
        engine = create_engine(f'ibm_db_sa+pyodbc:///?odbc_connect={quoted}')

        # 接続
        with engine.connect() as con:
            table_exist = check_exist_tabele_sql(table_name)

            df_te = pd.read_sql_query(
                table_exist,
                engine
            )
            
            try:
                if not df_te.empty:
                    con.execute(drop_sql(table_name))
                con.execute(cretae_sql)
                con.execute(insert_sql)
            except SQLAlchemyError as e:
                logger.error({
                    'action': 'btnUploadClick',
                    'message': 'SQLAlchemy error = %s' % e
                })
                err_flg = True
                con.close()

        logger.info("===END UPLOAD===")
    
    return err_flg




   
            





