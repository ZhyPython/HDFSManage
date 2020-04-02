import json
import re
from rest_framework.response import Response
from rest_framework.decorators import api_view

import requests

from .hdfs_monitor import HDFSMonitor
from .ssh import SSHClient


# namenode1和namenode2的ip
nn1 = "http://192.168.112.101:9870/jmx"
nn2 = "http://192.168.112.102:9870/jmx"

# Create your views here.
@api_view(['GET'])
def get_active_namenode(request):
    """获取active状态的namenode
    """
    context = {}
    response1 = requests.get(nn1 + "?qry=Hadoop:service=NameNode,name=NameNodeStatus")
    response2 = requests.get(nn2 + "?qry=Hadoop:service=NameNode,name=NameNodeStatus")
    if json.loads(response1.content)['beans'][0]['State'] == 'active':
        context.update({
            "active": "nn1"
        })
    elif json.loads(response2.content)['beans'][0]['State'] == "active":
        context.update({
            "active": "nn2"
        })
    else:
        context.update({
            "active": ""
        })
    return Response(context)

@api_view(['GET'])
def get_hdfs_metrics(request):
    """获取指标数据,get请求参数表示查询什么指标数值
    """
    metric_name = request.GET['metric']
    instance = HDFSMonitor()
    context = instance.hdfs_metrics(metric_name)
    return Response(context)

@api_view(['GET'])
def get_clusters(request):
    """获取当前系统中有多少集群
    """
    instance = HDFSMonitor()
    context = instance.list_clusters()
    # print(type(context))
    return Response(context)

@api_view(['GET'])
def get_datanodes(request):
    """获取集群中有多少台主机
    """
    context = {
        "info": ""
    }
    # 获取active的节点
    response1 = requests.get(nn1 + "?qry=Hadoop:service=NameNode,name=NameNodeStatus")
    response2 = requests.get(nn2 + "?qry=Hadoop:service=NameNode,name=NameNodeStatus")
    if json.loads(response1.content)['beans'][0]['State'] == 'active':
        # 查询包含datanodes信息的jmx
        info1 = requests.get(nn1 + "?qry=Hadoop:service=NameNode,name=NameNodeInfo")
        data = json.loads(info1.content)['beans'][0]['LiveNodes']
        # 获取的data为str，还需要进行 json.loads()才能进行解析
        dn_info = parse_dn_info(json.loads(data))
        context["info"] = dn_info
    elif json.loads(response2.content)['beans'][0]['State'] == "active":
        # 查询包含datanodes信息的jmx
        info2 = requests.get(nn2 + "?qry=Hadoop:service=NameNode,name=NameNodeInfo")
        data = json.loads(info2.content)['beans'][0]['LiveNodes']
        # 获取的data为str，还需要进行 json.loads()才能进行解析
        dn_info = parse_dn_info(json.loads(data))
        context["info"] = dn_info
    else:
        pass
    return Response(context)

def parse_dn_info(data):
    """解析datanodes的信息
    """
    # 新建dict存储信息, 以及正则表达式字符串
    dic = {}
    pattern = '^(.*?):'
    # 获取data中的键
    keys = data.keys()
    for key in keys:
        # 正则表达式获取键中的主机名
        temp = re.match(pattern, key).group(1)
        dic[temp] = re.match(pattern, data[key]['infoAddr']).group(1)
    return dic

@api_view(['GET'])
def dowanload_file_to_host(request):
    """将文件从hdfs上下载到某个主机
    """
    context = {}
    cli = SSHClient(request.GET['hostIP'])
    # 判断路径是否存在,不存在则创建
    cli.test_directory(request.GET['hostPath'])
    result, error = cli.exec_download_cmd(request.GET['filePath'],
                                          request.GET['hostPath'])
    context.update({
        'result': result,
        'error': error
    })
    return Response(context)
