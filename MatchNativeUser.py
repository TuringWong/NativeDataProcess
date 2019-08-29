# -*- coding: utf-8 -*-
# @Author: Turing Wong
# @Date:   2019-08-22 11:40:57
# @Last Modified time: 2019-08-22 15:06:21
# @E-mail: 173904839@qq.com
# @Desc: Be hungry, be foolish!


import pandas as pd
import numpy as np
import zipfile
import os
import sys
import time
from multiprocessing import Queue, Pipe, Process


def unZipDir(dir_, q_unzip_read):
    #开始循环导入处理
    print('开始解压...')
    for file in os.listdir(dir_):
        if file.endswith('.zip'):
            #解压文件
            z = zipfile.ZipFile(file, 'r')
            z.extractall(path='./input')
            file_segments = z.namelist()
            fa_dir = file_segments.pop(0)
            #修改解压后的目录名解决中文目录编码问题
            #os.rename('./input/' + fa_dir,'./input/' + fa_dir.encode('cp437').decode('gbk') )
            #队列中写入解压后的子文件名
            _ = q_unzip_read.put(file_segments)
            z.close()
            del z
    #队列结束标识
    _ = q_unzip_read.put('我收工啦')
    print('解压完成！')


def readFile(q_unzip_read,p_read,q_read_match):
    #读取native全量用户数
    data_native = pd.read_csv('./input/temp_native_all.csv',header =None,names=['mobile'])
    #生成pipe对象用于读文件进程与匹配进程间共享data_native
    #(con1, con2) = Pipe()
    #将data_native塞入Pipe
    p_read.send(data_native.head(10000000))
    p_read.close()
    del data_native
    #读取对列中的文件
    print('开始读文件...')
    while True:
        value = q_unzip_read.get(True)
        if value == '我收工啦':
            _ = q_read_match.put('我收工啦')
            print('文件读取完成！')
            break
        else:
            #读取文件
            for file_prefix in value:
                with open('./input/' + file_prefix) as f:
                    temp_data = pd.read_csv(f,header =None,names=['mobile'])
                #print(type(temp_data))
                #data_province = data_province.append(temp_data)
                #将读取的子文件塞入对列
                _ = q_read_match.put(temp_data)
                del temp_data
                #sys.exit()
                #删掉文件
                os.remove('./input/' + file_prefix)
            #写入省份文件读取结束标识，即正在处理的省份
            q_read_match.put('./input/' + os.path.dirname(value[0]))


def matchMobile(p_match, q_read_match):
    #获取data_native数据
    data_native = p_match.recv()
    #转换为集合
    print(len(data_native))
    data_native = set(data_native['mobile'])
    p_match.close()
    #进行匹配
    print('开始匹配...')
    data_result = []
    while True:
        data_province = q_read_match.get(True)
        if isinstance(data_province,str):
            # 全部省份处理完就结束
            if data_province == '我收工啦':
                print('匹配完成！')
                break
            else:
                #如果获取的是文件目录，意味着该省份的数据全部处理完，可以导出匹配结果文件
                pd.Series(data_result).to_csv(data_province + '/' + os.path.basename(data_province) + '.csv', index =False)
                data_result = []
        else:
            #将读入的数据与data_native进行匹配
            #print('data_result 长度：%d' % len(data_result))
            data_result.extend(list(data_native.intersection(set(data_province['mobile']))))


def main(dir_ = './input'):
    start_time = time.time()
    #生成两个队列和一个Pipe
    (p_read, p_match) = Pipe()
    q_unzip_read = Queue()
    q_read_match = Queue()
    #生成三个子进程
    unzip_process = Process(target = unZipDir, args = (dir_,q_unzip_read)) #解压进程
    read_process = Process(target = readFile, args = (q_unzip_read,p_read,q_read_match)) #读文件进程
    match_process = Process(target = matchMobile, args = (p_match,q_read_match)) #匹配进程
    #启动进程
    unzip_process.start()
    read_process.start()
    match_process.start()
    #进程等待
    unzip_process.join()
    read_process.join()
    match_process.join()

    #结束时间戳
    end_time = time.time()
    print('All Done!总共耗时：{0:.2f}s'.format(end_time - start_time))


if __name__ == '__main__':
    main()

