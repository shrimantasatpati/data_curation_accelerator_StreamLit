import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkFiles,SparkContext,SparkConf
from urllib.parse import urlparse
import streamlit as st
import pandas as pd
import os
import tempfile
# from data_curator import curate_data
import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkFiles,SparkContext,SparkConf
from urllib.parse import urlparse
import re
import streamlit as st

def check_uppercase(df):
    uppercase = [i for i in range(len(df.columns)) if df.columns[i] != df.columns[i].lower()]
    return uppercase

def check_space(df):
    columns_with_space = [i for i in range(len(df.columns)) if df.columns[i] != "".join(df.columns[i].split())]
    return columns_with_space

def check_for_special_characters(df):
    special_col = [i for i in range(len(df.columns)) if re.search(r"[^a-zA-z_ 0-9]", df.columns[i])]
    return special_col

def implement_header_validation(df):
    output = {}
    uppercase_columns = check_uppercase(df)
    if len(uppercase_columns) > 0:
        output["Have Uppercase"] = uppercase_columns

    columns_with_space = check_space(df)
    if len(columns_with_space) > 0:
        output["Have Space"] = columns_with_space

    special_columns = check_for_special_characters(df)
    if len(special_columns) > 0:
        output["Have Special Characters"] = special_columns
    return output

import re

import datetime


def header_small(uppercase_columns, df):
    for i in uppercase_columns:
        df = df.withColumnRenamed(df.columns[i], df.columns[i].lower())
    return df

def remove_spaces_add_underscore(columns_with_space, df):
    for i in columns_with_space:
        df = df.withColumnRenamed(df.columns[i], "_".join(df.columns[i].split()))
    return df

def remove_special_characters(special_col, df):
    for i in special_col:
        new = re.sub("[^a-zA-Z_0-9]", "", df.columns[i])
        df = df.withColumnRenamed(df.columns[i], new)
    return df

def change_header(df, count1=None):
    h_none = []
    dummy = []
    for h, i in enumerate(df.columns):
        if re.match(r"^_c\d{1,4}$", i) or i == "":
            if not df.filter(col(i).isNotNull()).count() >= 1:
                df = df.drop(i)
                dummy.append(h)
                continue
            df = df.withColumnRenamed(i, "unnamed_" + str(h))
            h_none.append(h)
    return df, h_none, dummy

def logs(part, message):
    now = datetime.datetime.now()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    return [timestamp, part, message]


def check_key(dic, key):
    return True if key in dic.keys() else False


def implementation_header_cleansing(header_cleansing, df):
    row = []
    if check_key(header_cleansing, "Have Uppercase"):
        df = header_small(header_cleansing["Have Uppercase"], df)
        row.append(logs("Header", "UpperCase in {} headers are changed to LowerCase ".format([df.columns[i] for i in header_cleansing["Have Uppercase"]])))

    if check_key(header_cleansing, "Have Space"):
        df = remove_spaces_add_underscore(header_cleansing["Have Space"], df)
        row.append(logs("Header","Leading and trailing spaces in {} headers are removed and in between spaces are replaced with underscore".format([df.columns[i] for i in header_cleansing["Have Space"]])))

    if check_key(header_cleansing, "Have Special Characters"):
        df = remove_special_characters(header_cleansing["Have Special Characters"], df)
        row.append(logs("Header", "Special Characters in {} headers are removed  ".format([df.columns[i] for i in header_cleansing["Have Special Characters"]])))

    return df, row

from pyspark.sql.functions import *
from pyspark.sql.types import *


def check_null_values(df):

    l = []
    for i in df.columns:
        if df.filter(col(i) == "").count() > 0:
            l.append(i)

    return l


def date_col_string(df,count1=None):
    date_columns = {}
    if count1 is None:
        count1=df.count()
    if count1<10000:
        df1=df
    else:
        df1=df.limit(10000)
    for col_name, data_type in df.dtypes:
        if data_type == 'date' or isinstance(data_type, DateType):
            if "datetype" not in date_columns.keys():
                date_columns["datetype"]=[]
            date_columns["datetype"].append(col_name)
            continue
        else:
            df1 = df1.withColumn("date check1", regexp_replace(col(col_name), r"[^0-9\/\-\a-z\.]", ""))
            df1 = df1.withColumn("date check2", col("date check1").rlike(
                r"^\d{4}[/-]\d{1,2}[/-]\d{1,2}$|^\d{1,2}[/-]\d{1,2}[/-]\d{4}$|^\d{1,2}[/-]\d{4}[/-]\d{1,2}$"))
            if df1.filter(col("date check2") == True).count() > 0:
                if "string" not in date_columns.keys():
                    date_columns["string"]=[]
                date_columns["string"].append(col_name)
                df = df.withColumn(col_name, regexp_replace(col(col_name), r"[^0-9\/\-]", ""))
    df1 = df1.drop("date check1", "date check2")
    return df, date_columns


def check_timestamp(df,count1):
    timestamp_columns = {}
    if count1 is None:
        count1=df.count()
    if count1<10000:
        df1=df
    else:
        df1=df.limit(10000)
    for col_name, data_type in df.dtypes:

        if data_type == 'timestamp' or isinstance(data_type, TimestampType):
            if "timestamp" not in timestamp_columns.keys():
                timestamp_columns['timestamp']=[]
            timestamp_columns['timestamp'].append(col_name)
            continue
        df1 = df1.withColumn("datetime check1", regexp_replace(col(col_name), r"[^0-9+TZ:./ \-\a-z]", ""))
        df1 = df1.withColumn("datetime check2", col("datetime check1").rlike(
            r"\d{4}[/-]\d{2}[-/]\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}[:]\d{2}|)"))
        if df1.filter(col("datetime check2") == True).count() > 0:
            if "string" not in timestamp_columns.keys():
                timestamp_columns["string"]=[]
            timestamp_columns["string"].append(col_name)
            df = df.withColumn(col_name, regexp_replace(col(col_name), r"[^0-9+TZ:./ -]", ""))
        df1 = df1.drop("datetime check2", "datetime check1")
    return df, timestamp_columns


def check_date(df, columnlist):
    if len(columnlist) >0:
        for i in columnlist.keys():
            for column in columnlist[i]:
                # iso_pattern = r'^\d{4}-\d{2}-\d{2}$'
                is_iso = to_date(col(column).cast('string'), "yyyy-MM-dd").isNotNull()
                # Add the new column to the DataFrame
                df = df.withColumn(column + '_is_iso', when(col(column)!=None, to_date(col(column).cast('string'), "yyyy-MM-dd").isNotNull()).otherwise(True))
                if df.filter(col(column + '_is_iso') == False).count() > 0 or i == "string":
                    df = df.drop(column + '_is_iso')
                else:
                    columnlist[i].remove(column)
                    df = df.drop(column + '_is_iso')
        return columnlist
    else:
        return columnlist


def check_time(df, columnlist):
    if len(columnlist) >0:
        for i in columnlist.keys():
            for column in columnlist[i]:
                iso_pattern = r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2}|)'
                is_iso = regexp_extract(col(column).cast('string'), iso_pattern, 0).isNotNull()
                # Add the new column to the DataFrame
                df = df.withColumn(column + '_is_iso', when(col(column)!= None, regexp_extract(col(column).cast('string'), iso_pattern, 0).isNotNull()).otherwise(True))
                if df.filter(col(column + '_is_iso') == False).count() > 0 or i == "string":
                    df = df.drop(column + '_is_iso')
                else:
                    columnlist[i].remove(column)
                    df = df.drop(column + '_is_iso')
        return columnlist
    else:
        return columnlist


def check_duplicates(df, primary_key=None, composite_keys=None,count1=None):
    if count1 is None:
        count1=df.count()
    if primary_key is not None:
        df=df.dropDuplicates([primary_key])
        count2=df.count()
        if count1 >count2 :
            return df,logs('Data', '{} Duplicates are removed'.format(count1-count2))
        else:
            return df,logs('Data', '0 Duplicates are removed')
    elif composite_keys is not None:
        df=df.dropDuplicates(composite_keys)
        count2=df.count()
        if count1 >count2:
            return df,logs('Data', '{} Duplicates are removed'.format(count1-count2))
        else:
            return df,logs('Data', '0 Duplicates are removed')
    else:
        df=df.distinct()
        count2=df.count()
        if count1 >count2 :
            return df,logs('Data', '{} Duplicates are removed'.format(count1-count2))
        else:
            return df,logs('Data', '0 Duplicates are removed')


def check_leading_trailing_spaces(df):
    l = []
    for i in df.columns:
        df = df.withColumn("hasspace", regexp_extract(col(i), r'^\s+|\s+$', 0).rlike(r'^\s+|\s+$'))
        if df.filter(col("hasspace") == True).count() > 0:
            l.append(i)
    df = df.drop("hasspace")
    return l

def place(df,list2,columns=None,reg_ex=None):
    vs={}
    if columns is None:
        columns=df.columns
    if reg_ex is None:
        reg_ex='[a-zA-Z0-9\@\;\:\,\/\\\.\_\ \-\.]'
    for i in list2.keys():
        if len(i)>1:
            m_reg='\\'.join(i.split(","))
            lt_reg='[\\'+'\\'.join(i.split(","))+']'

        else:
           lt_reg= m_reg='\{}'.format(i)
        if type(list2[i][0]) is str:
            if i not in vs.keys():
                    vs[i] = []
            reg_ex1 = '^({})?[a-zA-Z0-9]+'.format(lt_reg)+reg_ex+'*[a-zA-Z0-9]+({})?$'.format(lt_reg)
            for j in list2[i]:
                if df.where(col(j).rlike(reg_ex1)==False).count() > 0:
                    vs[i].append(j)
                columns.remove(j)
            continue
        if list2[i][0]==1:
            if i not in vs.keys():
                vs[i] = [1]
            reg_ex1 = '^({})?[a-zA-Z0-9]+'.format(lt_reg)+reg_ex+'*[a-zA-Z0-9]+$'
            for j in list2[i][1:]:
                if df.where(col(j).rlike(reg_ex1)==False).count() > 0:
                    vs[i].append(j)
                columns.remove(j)
            continue
        if list2[i][0]==2:
            if i not in vs.keys():
                    vs[i] = [2]
            reg_ex1 ='^[a-zA-Z0-9]+'+reg_ex[:-1] + "{}".format(str(m_reg)) + reg_ex[-1:]+'*[a-zA-Z0-9]+$'
            for j in list2[i][1:]:
                if df.where(col(j).rlike(reg_ex1)==False).count() > 0:
                    vs[i].append(j)
                columns.remove(j)
            continue
        if list2[i][0]==3:
            if i not in vs.keys():
                    vs[i] = [3]
            reg_ex1 = '^[a-zA-Z0-9]+'+reg_ex+'*[a-zA-Z0-9]+({})?$'.format(lt_reg)
            for j in list2[i][1:]:
                print(reg_ex1)
                if df.where(col(j).rlike(reg_ex1)==False).count() > 0:
                    vs[i].append(j)
                columns.remove(j)
            continue
        if list2[i][0]==4:
            if i not in vs.keys():
                    vs[i] = [4]
            reg_ex1 = '^({})?[a-zA-Z0-9]+'.format(lt_reg)+reg_ex[:-1] + "{}".format(str(m_reg)) + reg_ex[-1:]+'*[a-zA-Z0-9]+$'
            for j in list2[i][1:]:
                if df.where(col(j).rlike(reg_ex1)==False).count() > 0:
                    vs[i].append(j)
                columns.remove(j)
            continue
        if list2[i][0]==5:
            if i not in vs.keys():
                    vs[i] = [5]
            reg_ex1 = '^[a-zA-Z0-9]+'+reg_ex[:-1] + "{}".format(str(m_reg)) + reg_ex[-1:]+'*[a-zA-Z0-9]+({})?$'.format(lt_reg)
            for j in list2[i][1:]:
                if df.where(col(j).rlike(reg_ex1)==False).count() > 0:
                    vs[i].append(j)
                columns.remove(j)
            continue
        if list2[i][0]==6:
            if i not in vs.keys():
                    vs[i] = [6]
            reg_ex1 = '^({})?[a-zA-Z0-9]+'.format(lt_reg)+reg_ex[:-1] + "\{}".format(str(m_reg)) + reg_ex[-1:]+'*[a-zA-Z0-9]+({})?$'.format(lt_reg)
            for j in list2[i][1:]:
                if df.where(col(j).rlike(reg_ex1)==False).count() > 0:
                    vs[i].append(j)
                columns.remove(j)
    return columns,vs

def check_special_char(df, valid_across_allcol=None,date_columns=None, timestamp_columns=None, list2=None):
    columns = df.columns
    l = []
    vs={}
    reg_ex = r"[a-zA-Z0-9@\;\:\,\/\\\.\_\ \-]"
    if valid_across_allcol != None:
        for i in valid_across_allcol:
            reg_ex = reg_ex[:-1] + "\{}".format(str(i)) + reg_ex[-1:]
    if date_columns is not None and "string" in date_columns.keys():
        for i in date_columns["string"]:
            columns.remove(i)
    if timestamp_columns is not None and "string" in timestamp_columns.keys():
        for i in timestamp_columns["string"]:
            columns.remove(i)
    if list2 is not None:
        columns,vs=place(df,list2,columns,reg_ex)
    reg_ex1='^[a-zA-Z0-9]+'+reg_ex+'*[a-zA-Z0-9]'
    for i in columns:
        if df.where(col(i).rlike(reg_ex1)==False).count() > 0:
            l.append(i)
    return l,vs, reg_ex

def not_valid_primarykey_compositekey(df, primary_key=None, composite_keys=None):
    output = {}
    if composite_keys != None:
        df = df.withColumn("composite_key",
                           concat_ws("", *[coalesce(col(c).cast(StringType()), lit("")) for c in composite_keys]))
        if df.filter(col("composite_key") == "").count() > 0:
            output["Valid Composite key"] = "not valid as they have atleast 1 null value in combination"

        else:
            output["Valid Composite key"] = "valid as they don't have any null value in combination"
    else:
        output["Valid Composite key"] = "not passed/given"
    if primary_key != None:
        if df.filter(col(primary_key).isNull()).count() > 0:
            output["Valid Primary key"] = "not valid as it does have atleast 1 null value"
        else:
            output["Valid Primary key"] = "valid as it doesn't have any null value"
    else:
        output["Valid Primary key"] = "not passed/given"
    return output
def check_datatype(df):
    Integer=[]
    Float=[]
    for i, data_type in df.dtypes:
       if data_type == 'string' or isinstance(data_type, StringType):
           if df.where(col(i).rlike("^[0-9]*\.[0-9]*$")==False).count()==0:
               Float.append(i)
               continue
           if df.where(col(i).rlike("^[0-9]*$")==False).count()==0:
               Integer.append(i)
               continue
    return Integer,Float


def azure_connection(path,azure_blob):
    conf = SparkConf()
    conf.set("spark.jars.packages",
             "org.apache.hadoop:hadoop-azure:3.3.5,com.microsoft.azure:azure-storage:8.6.6,com.azure:azure-storage-blob:12.24.0")
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Read CSV") \
        .config(conf=conf) \
        .config("fs.azure.account.key.{}.blob.core.windows.net".format(azure_blob["storage_account_name"]), azure_blob["storage_account_key"]) \
        .getOrCreate()
    df = spark.read.format('csv').option('header', True).load("wasbs://{}@{}.blob.core.windows.net/{}".format(azure_blob["container_name"],azure_blob["storage_account_name"],path))
    return df

import datetime

def change_datatype(df,Integer,Float):
    for i in Integer:
        df=df.withColumn(i,col(i).cast("integer"))
    for i in Float:
        df=df.withColumn(i,col(i).cast("float"))
    return df

def trim_leading_trailing_spaces(df, column_names):
    for i in column_names:
        df = df.withColumn(i, trim(df[i]))
    return df


def missing_values(df, column_names):
    for i in column_names:
        df = df.withColumn(i, when(col(i).isNull(), None).when(col(i) == '', None).otherwise(col(i)))
    return df


def drop_duplicates(df, primary_key=None, composite_keys=None):
    if primary_key != None:
        return df.dropDuplicates([primary_key])
    if composite_keys != None:
        return df.dropDuplicates(composite_keys)
    else:
        return df.distinct()



def date_iso(df, date_columns):
    if "string" in date_columns.keys() and len(date_columns["string"]) > 0:
        for i in date_columns["string"]:
            df = df.withColumn(i, when(to_date(col(i), 'd/M/yyyy').isNotNull(), to_date(col(i), 'd/M/yyyy'))
                               .when(to_date(col(i), 'M/d/yyyy').isNotNull(), to_date(col(i), 'M/d/yyyy'))
                               .when(to_date(col(i), 'MM/dd/yyyy').isNotNull(), to_date(col(i), 'MM/dd/yyyy'))
                               .when(to_date(col(i), 'M-d-yyyy').isNotNull(), to_date(col(i), 'M-d-yyyy'))
                               .when(to_date(col(i), 'd-M-yyyy').isNotNull(), to_date(col(i), 'd-M-yyyy'))
                               .when(to_date(col(i), 'dd-MM-yyyy').isNotNull(), to_date(col(i), 'dd-MM-yyyy'))
                               .when(to_date(col(i), 'dd/MM/yyyy').isNotNull(), to_date(col(i), 'dd/MM/yyyy'))
                               .when(to_date(col(i), 'yyyy/MM/dd').isNotNull(), to_date(col(i), 'yyyy/MM/dd'))
                               .when(to_date(col(i), 'yyyy/dd/MM').isNotNull(), to_date(col(i), 'yyyy/dd/MM'))
                               .when(to_date(col(i), 'yyyy-MM-dd').isNotNull(), to_date(col(i), 'yyyy-MM-dd'))
                               .when(to_date(col(i), 'yyyy-dd-MM').isNotNull(), to_date(col(i), 'yyyy-dd-MM'))
                               .otherwise(col(i)))
            df = df.withColumn(i, to_date(col(i)))

    if "datetype" in date_columns.keys() and len(date_columns["datetype"]):
        for i in date_columns["datetype"]:
            df = df.withColumn(i, date_format(col(i), "yyyy-mm-dd"))
            df = df.withColumn(i, to_date(col(i)))
    return df

def place_remove(df,list2,reg_ex=None):
    if reg_ex is None:
        reg_ex='[^a-zA-Z0-9\@\;\:\,\/\\\.\_\ \-\.]'
    for i in list2.keys():
        if len(i)>1:
            m_reg='\\'.join(i.split(","))
            lt_reg='[\\'+'\\'.join(i.split(","))+']'
        else:
           lt_reg= m_reg='\{}'.format(i)
        if type(list2[i][0]) is str:
            for j in list2[i]:
                df = df.withColumn(j, when(col(j).isNull(),None).otherwise(concat_ws("",regexp_extract(regexp_extract(col(j), r'^[^a-zA-Z0-9]*', 0), lt_reg,0),regexp_replace(regexp_replace(col(j), '^[^a-zA-Z0-9]*|[^a-zA-Z0-9]*$', ''), reg_ex,""),regexp_extract(regexp_extract(col(j), r'[^a-zA-Z0-9]*$', 0), lt_reg,0))))
                df.show()
            continue
        if list2[i][0]==1:

            for j in list2[i][1:]:
                df = df.withColumn(j, when(col(j).isNull(),None).otherwise(concat_ws("",regexp_extract(regexp_extract(col(j), r'^[^a-zA-Z0-9]*', 0), lt_reg,0),regexp_replace(regexp_replace(col(j), '^[^a-zA-Z0-9]*|[^a-zA-Z0-9]*$', ''), reg_ex,""))))
            continue
        if list2[i][0]==2:
            reg_ex1 = reg_ex[:-1] + m_reg + reg_ex[-1:]
            for j in list2[i][1:]:
                df = df.withColumn(j, when(col(j).isNull(),None).otherwise(regexp_replace(regexp_replace(col(j), '^[^a-zA-Z0-9]*|[^a-zA-Z0-9]*$', ''), reg_ex1, "")))
            continue
        if list2[i][0]==3:
            for j in list2[i][1:]:
                df = df.withColumn(j,when(col(j).isNull(),None).otherwise(concat_ws("",regexp_replace(regexp_replace(col(j), '^[^a-zA-Z0-9]*|[^a-zA-Z0-9]*$', ''), reg_ex,""),regexp_extract(regexp_extract(col(j), r'[^a-zA-Z0-9]*$', 0), lt_reg,0))))
            continue
        if list2[i][0]==4:
            reg_ex1 = reg_ex[:-1] + m_reg + reg_ex[-1:]
            for j in list2[i][1:]:
                df = df.withColumn(j,when(col(j).isNull(),None).otherwise(concat_ws("",
                                                regexp_extract(regexp_extract(col(j), r'^[^a-zA-Z0-9]*', 0), lt_reg,
                                                               0),
                                                regexp_replace(regexp_replace(col(j), '^[^a-zA-Z0-9]*|[^a-zA-Z0-9]*$', ''), reg_ex1,
                                                               ""))))
            continue
        if list2[i][0]==5:
            reg_ex1 = reg_ex[:-1] + m_reg + reg_ex[-1:]
            for j in list2[i][1:]:
                df = df.withColumn(j,when(col(j).isNull(),None).otherwise(concat_ws("",regexp_replace(regexp_replace(col(j), '^[^a-zA-Z0-9]*|[^a-zA-Z0-9]*$', ''), reg_ex1,""),regexp_extract(regexp_extract(col(j), r'[^a-zA-Z0-9]*$', 0), lt_reg,0))))
            continue
        if list2[i][0]==6:
            reg_ex1 = reg_ex[:-1] + m_reg + reg_ex[-1:]
            for j in list2[i][1:]:
                df = df.withColumn(j,when(col(j).isNull(),None).otherwise(concat_ws("",
                                                regexp_extract(regexp_extract(col(j), r'^[^a-zA-Z0-9]*', 0), lt_reg,
                                                               0),
                                                regexp_replace(regexp_replace(col(j), '^[^a-zA-Z0-9]*|[^a-zA-Z0-9]*$', ''), reg_ex1,
                                                               ""),
                                                regexp_extract(regexp_extract(col(j), r'[^a-zA-Z0-9]*$', 0), lt_reg,
                                                               0))))
    return df



def remove_l_t_valid_special_characters(list1, df, list2=None, reg_ex=None,valid_across_allcol=None):
    """list 2 is from config """  # {"@":[col_name,col_name]}
    # """ list 1 will have column names that contain leading and trailing special chars leaving the list2 columns   """
    if reg_ex is None:
        reg_ex = r"[^a-zA-Z0-9@\;\:\,\/\\\.\_\ \-]"
        if valid_across_allcol != None:
            for i in valid_across_allcol:
                reg_ex = reg_ex[:-1] + "\{}".format(str(i)) + reg_ex[-1:]

    if list2 is not None:

        df=place_remove(df,list2,reg_ex)

        if len(list1) > 0:
            for i in list1:
                df = df.withColumn(i, regexp_replace(col(i), r'^[^a-zA-Z0-9]*|[^a-zA-Z0-9@\;\:\,\/\\\.\_\ \-]|[^a-zA-Z0-9]+$', ''))
        return df
    else:
        for i in list1:
            df = df.withColumn(i, regexp_replace(col(i), r'^[^a-zA-Z0-9]*|{}|[^a-zA-Z0-9]+$'.format(reg_ex), ''))
        return df


def to_iso(df, timestamp_columns, inputtz=None, outputtz=None):
    if inputtz is None:
        inputtz = "GMT"
    if outputtz is None:
        outputtz = "UTC"
    output_format = 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\''
    collist = []
    for i in timestamp_columns.values():
        collist += i
    for colname in collist:
        # Convert the input timestamp to UTC
        utc_time = to_utc_timestamp(col(colname), inputtz)
        # Convert the UTC timestamp to the output format
        iso_time = from_utc_timestamp(utc_time, outputtz).cast('string')
        # Remove any trailing zeros from the seconds
        iso_time = regexp_replace(iso_time, '\.0*Z$', 'Z')
        # Add the new column to the DataFrame
        df = df.withColumn(colname, to_timestamp(date_format(iso_time, output_format)))
        df = df.withColumn(colname, to_timestamp(colname))
    return df


def check_key(dic, key):
    return True if key in dic.keys() else False


def logs(part, message):
    now = datetime.datetime.now()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    return [timestamp, part, message]

def curate_data(path, logging_path=None, output_file_path=None, primary_key=None, composite_keys=None,
                valid_across_allcol=None, valid_spl_chars=None, inputtz=None, outputtz=None, aws_s3=None, azure_blob=None):
    try:
        if path is None:
            raise ValueError("No path given.")

        # Read the dataframe based on the path type
        if urlparse(path).scheme in ['https', 'http']:
            spark = SparkSession.builder.appName("Read CSV").getOrCreate()
            spark.sparkContext.addFile(path)
            df = spark.read.format("csv").option("header", True).load("file://" + SparkFiles.get(
                re.search(r"/([^/]+\.csv)$", path).group(1) if re.search(r"/([^/]+\.csv)$", path) else None))
        elif urlparse(path).scheme == 's3':
            conf = SparkConf()
            conf.set("spark.jars.packages",
                     "org.apache.spark:spark-hadoop-cloud_2.12:2.12.14,org.apache.hadoop:hadoop-aws:3.0.0,org.apache.hadoop:hadoop-client:3.0.0,org.apache.hadoop:hadoop-common:3.0.0,com.amazonaws:aws-java-sdk-core:2.20.97,com.amazonaws:aws-java-sdk-kms:1.12.490,com.amazonaws:aws-java-sdk-s3:1.12.467")
            spark = SparkSession.builder \
                .master("local[1]") \
                .appName("Read CSV") \
                .config(conf=conf) \
                .config("fs.s3a.access.key", aws_s3["access_key"]) \
                .config("fs.s3a.secret.key", aws_s3["secret_key"]) \
                .config("fs.s3a.endpoint", "s3.amazonaws.com") \
                .getOrCreate()
            path = "s3a" + path[2:]
            df = spark.read.format("csv").option("header", True).load(path)
        elif azure_blob is not None:
            df = azure_connection(path, azure_blob)
        else:
            spark = SparkSession.builder.appName("Read CSV").getOrCreate()
            df = spark.read.format("csv").option("header", True).load(path)

        if df.rdd.isEmpty():
            raise ValueError("File is empty")
        count1 = df.count()
        if df.columns and count1 == 0:
            raise ValueError("DataFrame has only column names and no data.")

        if all(header == "" for header in df.columns):
            raise Exception("All headers are null.")

        # Process valid_spl_chars
        if valid_spl_chars is not None:
            for key in valid_spl_chars.keys():
                if isinstance(valid_spl_chars[key][0], str):
                    valid_spl_chars[key] = ["%Cache\\Value&#"] + valid_spl_chars[key]

        # Process columns
        for i, column_name in enumerate(df.columns):
            if primary_key is not None and primary_key == column_name:
                primary_key = i
            if composite_keys is not None and column_name in composite_keys:
                composite_keys[composite_keys.index(column_name)] = i
            if valid_spl_chars is not None:
                for key in valid_spl_chars.keys():
                    if column_name in valid_spl_chars[key]:
                        valid_spl_chars[key][valid_spl_chars[key].index(column_name)] = i

        # Header validation and cleansing
        header_cleansing_columns = implement_header_validation(df)
        if len(header_cleansing_columns) > 0:
            df, row = implementation_header_cleansing(header_cleansing_columns, df)
        else:
            print("There is no Header cleansing Required")
            now = datetime.datetime.now()
            ts = now.strftime("%Y-%m-%d %H:%M:%S")
            row = [[ts, "Header", "There is no Header cleansing Required"]]

        df, h_none, dummy = change_header(df, count1)
        if h_none is not None:
            now = datetime.datetime.now()
            ts = now.strftime("%Y-%m-%d %H:%M:%S")
            row.append([ts, "Header",
                        "Null header names of indices {} are changed to appropriate name(unnamed_index)".format(h_none)])
        if dummy is not None:
            now = datetime.datetime.now()
            ts = now.strftime("%Y-%m-%d %H:%M:%S")
            row.append([ts, "Header",
                        "Headers that are null & data of that column is entirely null are in indices {} are deleted".format(
                            dummy)])

        # Update column references
        for i, column in enumerate(df.columns):
            if primary_key is not None and primary_key == i:
                primary_key = column
            if composite_keys is not None and i in composite_keys:
                composite_keys[composite_keys.index(i)] = column
            if valid_spl_chars is not None:
                for key in valid_spl_chars.keys():
                    if i in valid_spl_chars[key][1:]:
                        valid_spl_chars[key][valid_spl_chars[key].index(i)] = column

        # Remove cache values from valid_spl_chars
        for key in valid_spl_chars.keys():
            if valid_spl_chars[key][0] == "%Cache\\Value&#":
                valid_spl_chars[key] = valid_spl_chars[key][1:]

        rows = []

        # Date and timestamp processing
        df, date_columns = date_col_string(df, count1)
        df, timestamp_columns = check_timestamp(df, count1)

        # Space trimming
        space_col = check_leading_trailing_spaces(df)
        if len(space_col) > 0:
            df = trim_leading_trailing_spaces(df, space_col)
            rows.append(logs("Data", "Leading and trailing spaces in {} columns are removed".format(space_col)))

        # Special character processing
        spl_char_col, v_s, reg_ex = check_special_char(df, valid_across_allcol, date_columns, timestamp_columns, valid_spl_chars)
        cache = list(v_s.keys())
        for i in cache:
            if len(v_s[i]) < 1:
                v_s.pop(i)
            elif len(v_s[i]) < 2 and isinstance(v_s[i][0], int):
                v_s.pop(i)

        if len(spl_char_col) > 0 or len(v_s) > 0:
            df = remove_l_t_valid_special_characters(spl_char_col, df, v_s, None, valid_across_allcol)
            rows.append(logs("Data", "Invalid Special_characters in {} columns are removed".format(spl_char_col)))
            rows.append(logs("Data",
                             "Leading and trailing invalid special characters in {} columns are removed and the key(special Character) of dictionary is kept in the leading and trailing according to the {} ".format(
                                 v_s, valid_spl_chars)))

        # Null value processing
        null_col = check_null_values(df)
        if len(null_col) > 0:
            df = missing_values(df, null_col)
            rows.append(logs("Data", "Missing values in {} columns are replaced with null".format(null_col)))

        # Date format processing
        if len(date_columns) > 0:
            change_date = check_date(df, date_columns)
            if len(change_date["string"]) > 0 or len(change_date["datetype"]) > 0:
                df = date_iso(df, change_date)
                rows.append(logs("Data", "Date format in {} columns are changed to iso format".format(change_date)))

        # Timestamp processing
        if len(timestamp_columns) > 0:
            change_datetime = check_time(df, timestamp_columns)
            if len(change_datetime["string"]) > 0 or len(change_datetime["timestamp"]) > 0:
                df = to_iso(df, change_datetime, inputtz, outputtz)
                rows.append(
                    logs("Data", "Date Time format in {} columns are changed to iso format".format(change_datetime)))

        # Datatype processing
        Integer, Float = check_datatype(df)
        if len(Integer) > 0 or len(Float) > 0:
            df = change_datatype(df, Integer, Float)
            if len(Integer) > 0:
                rows.append(logs("Data", "{} columns are changed from string to integer format".format(Integer)))
            if len(Float) > 0:
                rows.append(logs("Data", "{} columns are changed from string to float format".format(Float)))

        # Primary and composite key validation
        output = not_valid_primarykey_compositekey(df, primary_key, composite_keys)
        rows.append(logs("Data", "Primary Key is {}".format(output["Valid Primary key"])))
        rows.append(logs("Data", "Composite Key is {}".format(output["Valid Composite key"])))

        # Duplicate checking
        df, x = check_duplicates(df, primary_key, composite_keys, count1)
        rows.append(x)

        if len(rows) == 0:
            print("There is no Data cleansing Required")
            now = datetime.datetime.now()
            ts = now.strftime("%Y-%m-%d %H:%M:%S")
            rows = [[ts, 'Data', 'There is no Data cleansing Required']]

        rows = row + rows
        now = datetime.datetime.now()
        ts = now.strftime("%Y/%m/%d/%H/%M_%S")
        d = spark.createDataFrame(rows, ["Timestamp", "Cleansing Part", "Message"])

        # Output handling
        if logging_path is None:
            if output_file_path is None:
                return df, d
            file_name = os.path.basename(path)
            df.write.parquet(output_file_path + f"output/{ts}" + file_name[:-4])
            return d
        else:
            file_name = os.path.basename(path)
            output_file = f"logfile/{ts}_{file_name[:-4]}"
            d.coalesce(1).write.option("header", True).csv(logging_path + output_file)
            if output_file_path is None:
                return df
            file_name = os.path.basename(path)
            df.write.option("header", "true").mode("overwrite").parquet(output_file_path + f"output/{ts}" + file_name[:-4])

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise

import streamlit as st
import pandas as pd
import tempfile
import os
import ast
# from pyspark.sql import SparkSession

def main():
    st.title("Data Curation Accelerator")

    robot_icon_url = 'https://banner2.cleanpng.com/20181227/vei/kisspng-robotic-process-automation-business-process-automa-solar-14-1-5c247df1168260.3071323815458954090922.jpg'

    st.sidebar.markdown(f"""
    <div style="display: flex; align-items: center;">
        <img src="{robot_icon_url}" width="80" height="80" style="margin-right: 10px;">
        <h2 style="margin: 0; font-size: 24px;">DCA</h2>
    </div>
    """, unsafe_allow_html=True)

    st.sidebar.header("Curation Parameters")

    # Checkboxes for parameters
    use_primary_key = st.sidebar.checkbox("Use Primary Key")
    use_composite_keys = st.sidebar.checkbox("Use Composite Keys")
    use_special_chars = st.sidebar.checkbox("Handle Special Characters")
    use_valid_across_allcol = st.sidebar.checkbox("Use Valid Characters Across All Columns")
    use_input_tz = st.sidebar.checkbox("Use Input Timezone")
    use_output_tz = st.sidebar.checkbox("Use Output Timezone")
    use_aws_s3 = st.sidebar.checkbox("Use AWS S3 Connection")
    use_azure_blob = st.sidebar.checkbox("Use Azure Blob Connection")

    # Initialize parameter variables
    primary_key = None
    composite_keys = None
    valid_spl_chars = None
    valid_across_allcol = None
    input_tz = None
    output_tz = None
    aws_s3 = None
    azure_blob = None

    # Input fields based on checkbox selection
    if use_primary_key:
        primary_key = st.sidebar.text_input("Primary Key without quotes (example: EMPLOYEE_ID)")

    if use_composite_keys:
        composite_keys = st.sidebar.text_input("Composite Keys (comma-separated, no quotes and no spaces) (example: EMPLOYEE_ID,FIRST_NAME,LAST_NAME)")

    if use_special_chars:
        valid_spl_chars_input = st.sidebar.text_area(
            "Valid special characters and columns (e.g., {'!':['FIRST_NAME']})"
        )
        if valid_spl_chars_input:
            try:
                valid_spl_chars = ast.literal_eval(valid_spl_chars_input)
                if not isinstance(valid_spl_chars, dict):
                    st.sidebar.error("Invalid format. Please enter a valid dictionary.")
                    valid_spl_chars = None
            except (ValueError, SyntaxError):
                st.sidebar.error("Error parsing special characters input. Please check the format.")
                valid_spl_chars = None

    if use_valid_across_allcol:
        valid_across_allcol = st.sidebar.text_input("Valid Characters Across All Columns (e.g. @ ,)")

    if use_input_tz:
        input_tz = st.sidebar.text_input("Input Timezone (e.g., UTC)")

    if use_output_tz:
        output_tz = st.sidebar.text_input("Output Timezone (e.g., UTC or America/New_York)")

    if use_aws_s3:
        aws_s3 = {
            "access_key": st.sidebar.text_input("AWS Access Key"),
            "secret_key": st.sidebar.text_input("AWS Secret Key", type="password"),
        }

    if use_azure_blob:
        azure_blob = {
            "account_name": st.sidebar.text_input("Azure Blob Account Name"),
            "account_key": st.sidebar.text_input("Azure Blob Account Key", type="password"),
        }

    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

    if uploaded_file is not None:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_file_path = tmp_file.name

        st.success("File uploaded successfully")

        df_preview = pd.read_csv(tmp_file_path)
        st.write("Data Preview:")
        st.dataframe(df_preview.head())

        if st.sidebar.button("Curate Data"):
            # Process parameters
            if use_composite_keys and composite_keys:
                composite_keys = [key.strip() for key in composite_keys.split(',')]

            # Call curate_data function
            try:

                curated_df, message = curate_data(
                    tmp_file_path,
                    primary_key=primary_key if primary_key else None,
                    composite_keys=composite_keys if composite_keys else None,
                    valid_spl_chars=valid_spl_chars if valid_spl_chars else {},
                    valid_across_allcol=valid_across_allcol if valid_across_allcol else None,
                    inputtz=input_tz if input_tz else None,
                    outputtz=output_tz if output_tz else None,
                    aws_s3=aws_s3 if aws_s3 else None,
                    azure_blob=azure_blob if azure_blob else None
                )

                st.success("Curation completed")
                st.write("Curated Data Preview:")
                pandas_df = curated_df.toPandas()
                st.dataframe(pandas_df.head())
                csv = pandas_df.to_csv(index=False)
                st.download_button(
                    label="Download curated data as CSV",
                    data=csv,
                    file_name="curated_data.csv",
                    mime="text/csv",
                )
                st.write("Logs table")
                st.dataframe(message.toPandas())

            except Exception as e:
                st.error(f"Error during data curation: {str(e)}")

        # Clean up the temporary file
        os.unlink(tmp_file_path)

if __name__ == "__main__":
    main()
