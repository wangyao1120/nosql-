import os
import  json

import pymysql

#数据库操作类
# db.where('sname="李婷"').table('student').fields("sno,sname").select()
# SQL = "SELECT %FIELDS% FROM %TABLE%  %WHERE% "

class Model:
    def __init__(self,host,user,password,db,charset,port=3306):
        self.host = host  #服务器地址
        self.user = user  #用户名
        self.password = password  #密码
        self.db = db   #链接的数据库
        self.charset = charset  #字符集
        self.port = port    #端口默认是3306
        self.sql  = ''   #保存sql语句
        self.table_name = 'student'  #表
        self.cache = None  #字段缓存

        #查询参数
        self.options = {
            'field'   : '*',
            'table'   : self.table_name,
            'where'   : '',
            'groupby' : '',
            'having'  : '',
            'orderby' : '',
            'limit'   : ''
        }

        #数据库链接
        self.__connect()

        #获取字段缓存
        self.cacheField()

        if self.cache:
            fields = ','.join(self.cache.values())
            self.options['field'] = fields


    #提取字段缓存
    def cacheField(self):
        #如果没有表名，不做字段缓存
        if not self.table_name:
            self.cache = None
            return
        path = './cache'
        #如果不存在cache目录
        if not os.path.exists(path):
            os.mkdir(path)

        #拼接文件名
        path += '/' + self.table_name + '.json'

        if os.path.exists(path): #判断文件是否存在
            with open(path,'r') as pf:
                self.cache = json.load(pf)  #读入字段缓存
        else:
            #查询表结构
            sql = ' desc ' + self.table_name
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            self.cache = {}
            #获取字段列表，把字段加入到cache中
            for i in range(len(result)):
                self.cache[i] = result[i]['Field']

            #生成缓存文件
            with open(path,'w') as pf:
                json.dump(self.cache,pf)



    def __del__(self):
        #断开链接
        self.__close()

    def __connect(self):
        """
        链接数据库
        :return:
        """
        self.conn = pymysql.Connect(host=self.host,
                                    user=self.user,
                                    password=self.password,
                                    database=self.db,
                                    charset=self.charset,
                                    port=self.port)
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)


    def __close(self):
        """
        关闭链接
        :return:
        """
        self.cursor.close()
        self.conn.close()

    def __str__(self):
        return "{} {} {}".format(self.db,self.conn,self.cursor)

    def where(self,condition):
        """
        设置查询条件
        :param condition:   "sname = 'tom' and sno = '101'"
        :return:
        """
        if not self.options['where']:
            self.options['where'] = " where " + condition
        else:
            self.options['where'] += ' and ' + condition

        return self

    def whereor(self,condition):
        """
                设置查询条件
                :param condition:   "sname = 'tom' and sno = '101'"
                :return:
                """
        if not self.options['where']:
            self.options['where'] = " where " + condition
        else:
            self.options['where'] += ' or ' + condition

        return self


    def field(self,data):
        """
        字段列表
        :param data: 字段列表
        :return:
        """
        self.options['field'] = data
        return self

    def table(self,tablename):
        """
        设置表名
        :param table_name:
        :return:
        """
        # if not self.options['table']:
        #     self.options['table'] = tablename
        # else:
        #     self.options['table'] += ' , ' + tablename
        self.options['table'] = tablename

        return  self

    def orderby(self,condition):
        """
        排序
        :param condition:
        :return:
        """
        if not self.options['orderby']:
            self.options['orderby'] = " order by " + condition
        else:
            self.options['orderby'] += " , " + condition
        return self

    def groupby(self,condition):
        """
        分组
        :param condition:
        :return:
        """
        if not self.options['groupby']:
            self.options['groupby'] = " group by " + condition
        else:
            self.options['groupby'] += " , " + condition

        return self

    def having(self,condition):
        if not self.options['having']:
            self.options['having'] = " having " + condition
        else:
            self.options['having'] += " , " + condition

    def limit(self,n,offset=None):
        """
        limit
        :param n: int  长度
        :param offset: int 偏移量
        :return:
        """
        if offset:  #如果传入两个值
            offset, n = n , offset
            self.options['limit'] = " limit " + str(offset) + ' , ' + str(n)
        else: #一个参数
            self.options['limit'] = " limit "  + str(n)

        return self


    def __init_options(self):
        self.options = {
            'field': '*',
            'table': self.table_name,
            'where': '',
            'groupby': '',
            'having': '',
            'orderby': '',
            'limit': ''
        }

    def select(self):
        """
        数据查询
        :return: 查询数据
        """
        sql = "SELECT {field} FROM {table} {where} {groupby} {having} {orderby} {limit}"
        sql = sql.format(
            field   = self.options['field'],
            table   = self.options['table'],
            where   = self.options['where'],
            groupby = self.options['groupby'],
            having  = self.options['having'],
            orderby = self.options['orderby'],
            limit   = self.options['limit']
        )
        self.sql = sql
        return self.query(sql)

    def query(self,sql):
        # print(sql)
        self.__init_options()  #还原参数字典
        try:
            self.cursor.execute(sql)
            return self.cursor.fetchall()
        except Exception as e:
            print(e)
            return  None

    def delete(self):
        """
        删除记录
        :return:
        """
        sql = "DELETE FROM {table} {where} "
        sql = sql.format(
            table   = self.options['table'],
            where   = self.options['where']
        )
        self.sql = sql

        return  self.execute(sql)

    def execute(self,sql):
        self.__init_options()  #初始化参数字典
        try:
            self.cursor.execute(sql)
            self.conn.commit()  #手动提交
            return True
        except Exception as e:
            print(e)
            self.conn.rollback()  #回退
            return False

    def add_quote(self,data):
        for key in data:
            #如果值是字符串，给两边添加''
            if isinstance(data[key],str):
                data[key] = "'" + data[key] + "'"

    def valid_field(self,data):
        """
        根据字段缓存过滤非法字段
        :param data: 字典
        :return:
        """
        fields = set(self.cache.values())
        keys = set(data)  #获取字典所有的键
        #获取合法字段的集合，也就是取两个集合的交集
        insect_data = fields & keys
        return {key:value for key,value in data.items() if key in insect_data }

    def insert(self,data):
        """
        插入记录
        :data : {'sno':"'012'",'sname':'tom'}
        :return:
        """
        #1.将字典中字符串两边添加''
        self.add_quote(data)

        #字段过滤
        data = self.valid_field(data)

        #2获取键和值的列表
        keys = ''
        values = ''
        for key in data:
            keys += key + ','
            values += data[key] + ','
        keys = keys.rstrip(',')
        values = values.rstrip(',')
        self.options['field'] = keys
        self.options['value'] = values

        sql = "INSERT INTO {table}({field})  VALUES({value})".format(
                table = self.options['table'],
                field = self.options['field'],
                value = self.options['value']
        )
        self.sql = sql
        return self.execute(sql)


if __name__ == '__main__':
    db = Model('localhost','root','123','student','utf8')
    # print(db)
    # db.where('sname="李婷"').where("sno='101'").table('student').select()
    # db.where('sname="李婷"').table('student').where("sno='101'").select()
    # result = db.where('sid > 0').orderby('sno desc').table('student').limit(0,2).select()
    # print(db.sql)
    # print(result)
    # db.where("sid > 10").delete()
    db.table('student').insert({'sno':'011','sname':'许天元','age':21})
    print(db.sql)

    # print(db.cache)