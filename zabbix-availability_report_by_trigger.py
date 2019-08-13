#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from datetime import datetime as d
import pymysql
from pyzabbix import ZabbixAPI

__author__ = 'Slagmaur'
__copyright__ = 'Copyright 2019, Moscow'
__email__ = 'slagmaur@ya.ru'
__version__ = '0.0.1'
__status__ = 'Development'

####################################################################################
"""
Скрипт получает то же, что и веб-интерфейс Zabbix (Reports -> Availability report -> By trigger template)
В текущей версии - без фильтрации по группе хостов
"""
REQUEST = {
    'templateid': 12345,  # ID шаблона
    'triggerid': 12345,  # ID триггера в шаблоне
    'filter_timesince': '20190812000000',  # YYYYMMDDHHMMSS
    'filter_timetill': '20190813000000'  # YYYYMMDDHHMMSS
}
CONFIG = {
    'db_ip': '11.11.11.11',
    'db_port': 3306,
    'db_user': 'user',
    'db_pass': 'password',
    'db_name': 'zabbix',
    'api_url': 'http://zabbix',
    'api_user': 'apiuser',
    'api_pass': 'secret'
}
####################################################################################


zapi = ZabbixAPI(CONFIG['api_url'])
zapi.login(CONFIG['api_user'], CONFIG['api_pass'])

conn = pymysql.connect(host=CONFIG['db_ip'],
                       port=CONFIG['db_port'],
                       user=CONFIG['db_user'],
                       passwd=CONFIG['db_pass'],
                       db=CONFIG['db_name'],
                       cursorclass=pymysql.cursors.DictCursor)
mysql = conn.cursor()


def calculate_availability(triggerid, starttime, endtime, mysql):
    starttime = int(d.strptime(starttime, '%Y%m%d%H%M%S').timestamp())
    endtime = int(d.strptime(endtime, '%Y%m%d%H%M%S').timestamp())
    startvalue = 0
    ret = {}
    if 0 < starttime < int(d.now().timestamp()):
        sql = """
        SELECT e.eventid,e.value
        FROM events e
        WHERE e.objectid={0}
            AND e.source=0
            AND e.object=0
            AND e.clock<{1}
        ORDER BY e.eventid DESC
        """.format(triggerid, starttime)
        mysql.execute(sql)
        row = mysql.fetchone()
        if row:
            startvalue = row['value']
            min_ = starttime

    sql = """
    SELECT COUNT(e.eventid) AS cnt,MIN(e.clock) AS min_clock,MAX(e.clock) AS max_clock
    FROM events e
    WHERE e.objectid={0}
        AND e.source=0
        AND e.object=0
        AND clock>={1}
        AND clock<={2}
    """.format(triggerid, starttime, endtime)
    mysql.execute(sql)
    dbevents = mysql.fetchone()
    if dbevents['cnt'] > 0:
        try:
            min_
        except NameError:
            min_ = dbevents['min_clock']
        max_ = dbevents['max_clock']
    else:
        ret['true_time'] = 0
        ret['false_time'] = 0
        if startvalue == 1:
            ret['true'] = 100
            ret['false'] = 0
        else:
            ret['true'] = 0
            ret['false'] = 100
        return ret

    state = startvalue
    truetime = 0
    falsetime = 0
    time = min_
    rows = 0
    try:
        max_
    except NameError:
        max_ = int(d.now().timestamp())

    sql = """
    SELECT e.eventid,e.clock,e.value
    FROM events e
    WHERE e.objectid={0}
        AND e.source=0
        AND e.object=0
        AND e.clock BETWEEN {1} AND {2}
    ORDER BY e.eventid
    """.format(triggerid, min_, max_)
    mysql.execute(sql)
    dbevents = mysql.fetchall()
    for row in dbevents:
        clock = row['clock']
        value = row['value']
        diff = clock - time
        time = clock
        if state == 0:
            falsetime += diff
            state = value
        elif state == 1:
            truetime += diff
            state = value
        rows += 1
    if rows == 0:
        sql = """
        SELECT t.* FROM triggers t WHERE t.triggerid={}
        """.format(triggerid)
        mysql.execute(sql)
        state = mysql.fetchone()['value']
    if state == 0:
        falsetime += endtime - time
    elif state == 1:
        truetime += endtime - time
    totaltime = truetime + falsetime
    if totaltime == 0:
        ret['true_time'] = 0
        ret['false_time'] = 0
        ret['true'] = 0
        ret['false'] = 0
    else:
        ret['true_time'] = truetime
        ret['false_time'] = falsetime
        ret['true'] = 100 * truetime / totaltime
        ret['false'] = 100 * falsetime / totaltime
    return ret


hosts = [int(x['hostid']) for x in zapi.host.get(templateids=REQUEST['templateid'])]
triggers = zapi.trigger.get(
    output=['triggerid', 'description', 'expression', 'value'],
    expandDescription=1,
    expandData=1,
    monitored=1,
    selectHosts='extend',
    filter={'templateid': REQUEST['triggerid']},
    hostids=hosts
)
triggers = sorted(triggers, key=lambda x: (x['hosts'][0]['host'], x['hosts'][0]['description']))

output = []
for trigger in triggers:
    entry = {}
    availability = calculate_availability(trigger['triggerid'],
                                          REQUEST['filter_timesince'],
                                          REQUEST['filter_timetill'],
                                          mysql)
    entry['host'] = trigger['hosts'][0]['name']
    entry['name'] = trigger['description']
    entry['problems'] = '{0:.5f}'.format(availability['true'])
    entry['ok'] = '{0:.5f}'.format(availability['false'])
    output.append(entry)
    
mysql.close()
conn.close()
zapi.user.logout()

for entry in output:
    print(entry)
