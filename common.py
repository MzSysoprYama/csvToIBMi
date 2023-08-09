from typing import List
import re
import unicodedata

# 全部半角ならtrue
def isalnum_ascii_re(s):  
    return True if re.fullmatch('[\d\w]+', s, re.ASCII) else False

def decimal_check(f:float)->tuple:
    f_abs = abs(f)
    f_l = str(f_abs).split('.')
    f_decimal = len(f_l[0]) + len(f_l[1])
    f_fraction = len(f_l[1])
    return f_decimal, f_fraction

def decimal_str(arr: List[float])->str:
    decimal_list = list(map(lambda x: decimal_check(x), arr))
    # 同じ要素でzipしてくれる、そこから要素ごとの最大を取る
    max_d_f = list(map(max, zip(*decimal_list)))
    return f"DECIMAL({max_d_f[0]},{max_d_f[1]})"

# -1など負の値の時の長さチェック
def int_check(i: int)-> int:
    i_abs = abs(i)
    i_l = str(i_abs)
    i_decimal = len(i_l)
    return i_decimal

def int_str(arr: List[int])->str:
    max_d = max(map(lambda v: int_check(v), arr))
    return f"DECIMAL({max_d})"

# 全角2文字で半角1文字でカウントする
def get_east_asian_width_count(text):
    count = 0
    for c in text:
        if unicodedata.east_asian_width(c) in 'FWA':
            count += 2
        else:
            count += 1
    return count

# IBMiのシフトインアウトを再現する関数
def ibmi_str_count(str: str) -> int:
    match = re.finditer(r'(?<=[^\x01-\x7E])\s(?=[^\x01-\x7E])', str)
    if match:
        for m in match:
            pre: str = m.group(0)
            post: str = pre.replace(' ','　')
            str = str.replace(pre, post)
            
    tmpStr: str = str + "_"    
    tmpStr2: str = ""
    l: int = 1
    # 1文字目と2文字目を比較していく
    # F - East Asian Full-width
    # W - East Asian Wide
    # Na - East Asian Narrow (Na)
    for i in str:
        j = unicodedata.east_asian_width(i)
        
        h = unicodedata.east_asian_width(tmpStr[l])
        # whitespaceならそのままにする
        if tmpStr[l] == ' ' or i == ' ':
            tmpStr2 += str[l-1]
        elif (j == 'Na' and h != 'Na') or (j == 'W' and h == 'Na') or (j == 'F' and h == 'Na'):
            tmpStr2 += str[l-1] + " "
        else:
            tmpStr2 += str[l-1]
        l += 1
    
    if unicodedata.east_asian_width(tmpStr2[0]) != 'Na':
        tmpStr2 = " " + tmpStr2
    if unicodedata.east_asian_width(tmpStr2[-1]) != 'Na':
        tmpStr2 = tmpStr2 + " "

    lngth = get_east_asian_width_count(tmpStr2)
    return lngth