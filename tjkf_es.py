#!/usr/bin/env python
#-*-coding:utf-8 -*-
import sys
import random
from hivedb.HiveEx import HiveEx
from elasticsearch import Elasticsearch
from datetime import datetime

reload(sys)
sys.setdefaultencoding( "utf-8" )

list = ['luyifeng@ganji.com','xianxueliang@ganji.com','liujunhong@ganji.com','xiqingqing@ganji.com','shiguangyuan@ganji.com']
i = random.randint(0,len(list)-1)
hiver = HiveEx()
user = list[i]
ip = 'hive.corp.ganji.com'
port = 13080
dt = sys.argv[1]
#set mapred.queue.name=test;

sql = u"""
select
round(sum_recharge,2) as sumRecharge
,round(sum_recharge_cash,2) as cashCharge
,round(sum_recharge_award,2) as sumRechargeAward 
,round(recharge_sum_amount,2) as blcChargeZrTotal
,recharge_count as countCharge
,award_expire_time as nextExpirBlcGift
,category_id as cate1
,major_category_id as cate2
,round(amount,2) as blcConsumeTotal
,round(cash_amount,2) as blcConsumeCash
,user_id as userId
,deal_time as lastTopupTime
,round(cash,2) as lastTopupValue
,begin_at as serviceStartTime
,end_at as serviceEndTime
,package_code as wlt
,round(moon_consume_amount,2) as blcConsumeDyTotal
,round(moon_consume_cash_amount,2) as blcConsumeDyCash
,round(cpc_bid_amount,2) as blcConsumeZrJz
,round(user_pay_amount,2) as blcConsumeZrZd
,round(extend_amount,2) as blcConsumeZrZn
,round(other_amount,2) as blcConsumeZrOther
,dt
from wt_58gj_tjkf_detail
where user_id > 1 
and dt = '%s'
""" % dt

print sql
print user

is_sucess ,msg = hiver.submit_sql(user,sql,ip,port)
print is_sucess
print msg


execution_type = 'select'

if execution_type == 'insert':
   pass
else:
   hiver.urlred(msg,execution_type)

print "sucess"

