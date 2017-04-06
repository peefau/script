# coding=utf-8
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import sys
import os
from optparse import OptionParser
from datetime import datetime
import subprocess as sub
import json
import time

reload(sys)
sys.setdefaultencoding( "utf-8" )

class loadDataToES:
    def __init__(self, field_desc, data_file, host, port, index, doc_type, delimiter, tmp_file, cut_off=10000):
        self.host = host
        self.port = port
        self.index = index
        self.doc_type = doc_type
        self.delimiter = delimiter
        self.tmp_file = tmp_file
        self.field_desc = field_desc
        self.data_file = data_file
        #self.index_header = '{"index":{"_index":"%s", "_type":"%s"}}' %(self.index, self.doc_type)
        self.update_stat_part = ''
        self.update_stat_all = ''
        self.cut_off = cut_off
        self.line_num = 0
        self.all_line_num = 0
        self.body_list_part = []
        self.body_list_all = []
        self.bulk_content = ''
        self.bulk_content_all = ''
        self.url = 'http://%s:%s/_bulk' %(self.host, self.port)
        self.set_columns = ("serviceStartTime", "serviceEndTime", "wlt","cate1","cate2")
        self.success = open('success_log.txt','w+')
        self.errors = open('errors_log.txt','w+')
    def load_data(self):
        '''
        expample data from the file:
        2015-09-24 09:17:29,memory_11601,123988
        '''
        self.body_list_part = []
        self.body_list_all = []
        self.line_num = 0
        self.all_line_num = 0
        self.pretty_print('INFO: loading data to es, host: %s, index: %s' %(self.host, self.index))
        self.parse_field()
        with open(self.data_file, 'r') as f_desc:
            for line in f_desc:
                self.do_line(line)
		#print "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                self.line_num += 1
                self.all_line_num +=1
                if self.line_num >= self.cut_off:
                    self.bulk_content = '\n'.join(self.body_list_part)
                    self.bulk_content += '\n'
                    self._load_data(self.bulk_content)
                    self.body_list_part = []
                    self.bulk_content_all = '\n'.join(self.body_list_all)
                    self.bulk_content_all += '\n'
                    self._load_data(self.bulk_content_all)
                    self.body_list_all = []
                    self.line_num = 0
            if self.line_num > 0:
                self.bulk_content = '\n'.join(self.body_list_part)
                self.bulk_content += '\n'
                self._load_data(self.bulk_content)
                self.bulk_content_all = '\n'.join(self.body_list_all)
                self.bulk_content_all += '\n'
                self._load_data(self.bulk_content_all)
        self.pretty_print("INFO: [%s] all lines parsed !" % self.all_line_num)

    def parse_field(self):
        fields_list = []
        fields_desc = self.field_desc.strip().split(',')
        for item in fields_desc:
            items = item.split('|')
            fields_list.append([items[0], items[1]])
        self.fields_list = fields_list
        self.field_len = len(fields_list)

    def do_line(self, line):
        fields = line.strip().split(self.delimiter)
        # print "line %s:field %s" % (len(fields),self.field_len)
        if len(fields) != self.field_len:
            self.pretty_print("ERROR: line %d not match fields" % self.line_num)
            return
        (part_body_tmp,body_tmp) = self.get_body(fields, self.fields_list)
        for iter in self.fields_list:
            if iter[0] in self.set_columns:
                self.update_stat_part = '{"update":{"_index":"%s","_type":"%s","_id":"%s"} }\n{"script" :"if(ctx._source.containsKey(\\"%s\\") && ctx._source.%s instanceof List){if( ctx._source.%s.contains(%s)){ctx.op=\\"none\\"}else{ctx._source.%s=%s}} else{ctx._source.%s=%s}","params" : {"%s":[%s]}} ' % (self.index, self.doc_type, body_tmp["userId"],iter[0],iter[0],iter[0],iter[0],iter[0],iter[0],iter[0],iter[0],iter[0],part_body_tmp[iter[0]])
                self.body_list_part.append(self.update_stat_part)
        self.update_stat_all = '{"update":{"_index":"%s","_type":"%s","_id":"%s"} }\n{"doc" : %s } ' %(self.index, self.doc_type, body_tmp["userId"],str(body_tmp).replace("'", '"'))
        self.body_list_all.append(self.update_stat_all)

    def _load_data(self,content):
        """
        windows环境下使用：
            exec_str = "curl -XPOST %s --data-binary \"@%s\"" % (self.url,self.tmp_file)
            p = sub.Popen(exec_str, stdout=sub.PIPE,shell=True)
        POSIX环境下使用：
            p = sub.Popen(['curl', '-s', '-XPOST', self.url, '--data-binary', "@" + self.tmp_file ], stdout=sub.PIPE)
        :type self: loadDataToES
        """
        open(self.tmp_file, 'w').write(content)
        p = sub.Popen(['curl', '-s', '-XPOST', self.url, '--data-binary', "@" + self.tmp_file ], stdout=sub.PIPE)
        for line in iter(p.stdout.readline, b''):
            ret_dict = json.loads(line)
            for i in range(len(ret_dict['items'])):
                if ret_dict['items'][i]['update']['status'] == 200:
                    # self.pretty_print("INFO: %s lines parsed with no errors, total cost %d ms." % (len(ret_dict['items']), ret_dict['took']))
                    tmp_suc = "%s#%s\n" % (ret_dict['items'][i]['update']['_id'], ret_dict['items'][i]['update']['status'])
                    self.success.write(tmp_suc)
                else:
                    # self.pretty_print("ERROR: %s lines parsed with some errors, total cost %d ms." % (len(ret_dict['items']), ret_dict['took']))
                    tmp_err = "%s#%s\n" % (ret_dict['items'][i]['update']['_id'], ret_dict['items'][i]['update']['error'])
                    self.errors.write(tmp_err)

    def pretty_print(self, str):
        print('%s %s' %(datetime.now(), str))

    def get_body(self, fields, fields_list):
        counter = 0
        body = {}
        part_body = {}
        while (counter < len(fields)):
            # if the data type is 'date', we will translate the value str to date
            # type
            #print "field: %s " % fields_list[counter][0]
            if fields_list[counter][1] == 'date':
                if fields_list[counter][0] in self.set_columns:
                    part_body[fields_list[counter][0]] = self.translate_str_to_date(fields[counter])
                else:
                    body[fields_list[counter][0]] = self.translate_str_to_date(fields[counter])
            # and if the data type is 'int', we translate it to int
            elif fields_list[counter][1] == 'int':
                if fields_list[counter][0] in self.set_columns:
                    part_body[fields_list[counter][0]] = self.translate_str_to_int(fields[counter])
                else:
                    body[fields_list[counter][0]] = self.translate_str_to_int(fields[counter])
            elif fields_list[counter][1] == 'float':
                if fields_list[counter][0] in self.set_columns:
                    part_body[fields_list[counter][0]] = self.translate_str_to_float(fields[counter])
                else:
                    body[fields_list[counter][0]] = self.translate_str_to_float(fields[counter])
            # other is defalut to be str
            else:
                if fields_list[counter][0] in self.set_columns:
                    part_body[fields_list[counter][0]] = fields[counter]
                else:
                    body[fields_list[counter][0]] = fields[counter]
            counter += 1
        # print(my_body)
        return (part_body,body)

    def translate_str_to_date(self, date_str):
        try:
            date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
            return date.isoformat()
        except:
            self.pretty_print("Unexpected error: %s" % (sys.exc_info()[0]))
            self.pretty_print("Failed to translate '%s' to date." % (date_str))
        return False


    def translate_str_to_int(self, num_str):
        try:
            return int(num_str)
        except:
            self.pretty_print("Failed to translate '%s' to int." % (num_str))
        return False


    def translate_str_to_float(self, num_str):
        try:
            return float(num_str)
        except:
            self.pretty_print("Failed to translate '%s' to int." % (num_str))
        return False
    def getNowTime(self):
        return time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
if __name__ == '__main__':
        dt = sys.argv[1]
        dataFile = "/home/ganji/HiveThrift/tjkf_update_es/tjkf_update_es_%s.txt" % dt
        # dataFile = "bsgs_new_2015-12-11.txt"
        host = '10.126.97.216'
        index = 'csc'
        port = '9200'
        docType = 'customer_info_gj'
        delimiter = '\t'
        # tmpFile = 'update_es_tmp.txt'
        tmpFile = 'update_es_tmp.txt'
        fieldDesc = '\
sumRecharge|float,\
cashCharge|float,\
sumRechargeAward|float,\
blcChargeZrTotal|float,\
countCharge|int,\
nextExpirBlcGift|string,\
cate1|string,\
cate2|string,\
blcConsumeZrTotal|float,\
blcConsumeZrCash|float,\
userId|int,\
lastTopupTime|string,\
lastTopupValue|float,\
serviceStartTime|string,\
serviceEndTime|string,\
wlt|string,\
blcConsumeDyTotal|float,\
blcConsumeDyCash|float,\
blcConsumeZrJz|float,\
blcConsumeZrZd|float,\
blcConsumeZrZn|float,\
blcConsumeZrOther|float,\
dt|string'
        loader = loadDataToES(field_desc=fieldDesc, data_file=dataFile,host=host, index=index,port=port,doc_type=docType,delimiter=delimiter,tmp_file=tmpFile,cut_off=10000)
        print "Start -> %s" % loader.getNowTime()
        loader.load_data()
        print "End -> %s" % loader.getNowTime()
