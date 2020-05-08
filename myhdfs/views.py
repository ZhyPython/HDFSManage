import json
import re
import os
import pprint

import requests
from django.http import HttpResponse
from rest_framework.response import Response
from rest_framework.decorators import api_view
from rest_framework.status import HTTP_200_OK, HTTP_403_FORBIDDEN, \
                                  HTTP_400_BAD_REQUEST

from .hdfs_monitor import HDFSMonitor
from .ssh import SSHClient
from .parse_xml import ParserConf
from .models import HistoryJob


# Create your views here.
@api_view(['GET'])
def get_hdfs_dir(request):
    """获取hdfs目录信息
    """
    host = "http://" + request.GET['activeNN'] + ":" + "9870"
    response = requests.get(host + request.GET['url'])
    data = json.loads(response.text)
    return Response(data)


@api_view(['PUT'])
def upload_file(request):
    """上传文件到hdfs
    """
    context = {}
    # 用requests库将文件上传到hdfs中
    url = "http://" + request.data['activeNN'] + ":" + "9870" + request.data['url']
    response = requests.put(url, data=request.data['file'])
    if response.status_code == 201:
        context.update({
            'info': 'success'
        })
    else:
        context.update({
            'info': 'error'
        })
    return Response(context)


@api_view(['GET'])
def download_file(request):
    """下载文件
    """
    host = "http://" + request.GET['activeNN'] + ":" + "9870"
    response = requests.get(host + request.GET['url'])
    return HttpResponse(response.content, content_type="application/octet-stream")


@api_view(['GET'])
def get_block_info(request):
    """获取块信息
    """
    host = "http://" + request.GET['activeNN'] + ":" + "9870"
    response = requests.get(host + request.GET['url'])
    data = json.loads(response.text)
    return Response(data)


@api_view(['PUT'])
def mkdir(request):
    """在hdfs上新建文件夹
    """
    host = "http://" + request.data['activeNN'] + ":" + "9870"
    response = requests.put(host + request.data['url'])
    if response.status_code == 200:
        return Response(status=HTTP_200_OK)
    return Response(status=HTTP_403_FORBIDDEN)


@api_view(['GET'])
def get_active_namenode(request):
    """获取active状态的namenode
    """
    context = {}
    # 获取一个集群中的namenode
    instance = HDFSMonitor()
    namenode_list = instance.list_namenodes(request.GET['clusterName'])
    # 两个namenode的IP链接
    nn1 = "http://" + namenode_list[0]['hostIP'] + ":" + "9870"
    nn2 = "http://" + namenode_list[1]['hostIP'] + ":" + "9870"
    response1 = requests.get(nn1 + "/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus")
    response2 = requests.get(nn2 + "/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus")
    if json.loads(response1.content)['beans'][0]['State'] == 'active':
        context.update(namenode_list[0])
    elif json.loads(response2.content)['beans'][0]['State'] == "active":
        context.update(namenode_list[1])
    else:
        pass
    return Response(context)


@api_view(['GET'])
def get_hdfs_metrics(request):
    """获取指标数据,get请求参数表示查询什么指标数值
    """
    metric_name = request.GET['metric']
    cluster_name = request.GET['clusterName']
    instance = HDFSMonitor()
    context = instance.hdfs_metrics(metric_name, cluster_name)
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
    context = {}
    # 获取一个集群中的namenode
    instance = HDFSMonitor()
    namenode_list = instance.list_namenodes(request.GET['clusterName'])
    # 两个namenode的IP链接
    nn1 = "http://" + namenode_list[0]['hostIP'] + ":" + "9870"
    nn2 = "http://" + namenode_list[1]['hostIP'] + ":" + "9870"
    # 获取active的节点
    response1 = requests.get(nn1 + "/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus")
    response2 = requests.get(nn2 + "/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus")
    if json.loads(response1.content)['beans'][0]['State'] == 'active':
        # 查询包含datanodes信息的jmx
        info1 = requests.get(nn1 + "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo")
        data = json.loads(info1.content)['beans'][0]['LiveNodes']
        # 获取的data为str，还需要进行 json.loads()才能进行解析
        dn_info = parse_dn_info(json.loads(data))
        context = dn_info
    elif json.loads(response2.content)['beans'][0]['State'] == "active":
        # 查询包含datanodes信息的jmx
        info2 = requests.get(nn2 + "/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo")
        data = json.loads(info2.content)['beans'][0]['LiveNodes']
        # 获取的data为str，还需要进行 json.loads()才能进行解析
        dn_info = parse_dn_info(json.loads(data))
        context = dn_info
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


@api_view(['DELETE'])
def delete_file(request):
    """删除文件或目录
    """
    host = "http://" + request.data['activeNN'] + ":" + "9870"
    response = requests.delete(host + request.data['url'])
    if response.status_code == 200:
        return Response(status=HTTP_200_OK)
    return Response(status=HTTP_403_FORBIDDEN)

@api_view(['PUT'])
def set_permission(request):
    """设置权限
    """
    host = "http://" + request.data['activeNN'] + ":" + "9870"
    response = requests.put(host + request.data['url'])
    if response.status_code == 200:
        return Response(status=HTTP_200_OK)
    data = json.loads(response.text)
    return Response(data, status=HTTP_400_BAD_REQUEST)

@api_view(['PUT'])
def set_owner(request):
    """设置所有者
    """
    host = "http://" + request.data['activeNN'] + ":" + "9870"
    response = requests.put(host + request.data['url'])
    if response.status_code == 200:
        return Response(status=HTTP_200_OK)
    data = json.loads(response.text)
    return Response(data, status=HTTP_400_BAD_REQUEST)

@api_view(['PUT'])
def set_group(request):
    """设置所有者
    """
    host = "http://" + request.data['activeNN'] + ":" + "9870"
    response = requests.put(host + request.data['url'])
    if response.status_code == 200:
        return Response(status=HTTP_200_OK)
    data = json.loads(response.text)
    return Response(data, status=HTTP_400_BAD_REQUEST)

@api_view(['PUT'])
def set_replication(request):
    """设置所有者
    """
    host = "http://" + request.data['activeNN'] + ":" + "9870"
    response = requests.put(host + request.data['url'])
    if response.status_code == 200:
        return Response(status=HTTP_200_OK)
    data = json.loads(response.text)
    return Response(data, status=HTTP_400_BAD_REQUEST)

@api_view(['GET'])
def validate_target_dir(request):
    """验证sqoop过程中的输出目录是否不存在
    """
    host = request.GET['activeNN']
    target_dir = request.GET['targetDir']
    url = "http://" + host + ":" + "9870" + "/webhdfs/v1" + target_dir + "?op=LISTSTATUS"
    # 将返回的信息序列化为一个对象
    response = json.loads(requests.get(url).text)
    # 定义返回的消息格式,flag的值表示target_dri是否可用，如果hdfs上不存在，则该目录可用
    context = {
        'flag': True
    }
    # 解析响应
    if response.get('FileStatuses'):
        context['flag'] = False
    return Response(context)

@api_view(['POST'])
def sqoop_import(request):
    """通过sqoop导入数据,判断用户是否为linux的用户，不是则添加
    """
    # 实例化ssh客户端
    cli = SSHClient(request.POST['hostIP'])
    context = cli.sqoop_pro(
        request.POST['dbType'],
        request.POST['hostIP'],
        request.POST['dbName'],
        request.POST['username'],
        request.POST['password'],
        request.POST['tableName'],
        request.POST['jobName'],
        request.POST['targetDir'],
        request.POST['mapNums'],
        request.POST['sysUser']
    )
    # 将任务id存入数据库
    if context['jobId'] != '':
        HistoryJob.add_job(context['jobId'], request.POST['sysUser'])
    return Response(context)

@api_view(['GET'])
def get_history_job_metrics(request):
    """通过数据库获取历史任务的ID，并通过yarn rest api拿到历史指标数据
    """
    # 获取数据库中的jobs
    jobs = HistoryJob.get_all()
    # 获取一个集群中的namenode
    instance = HDFSMonitor()
    namenode_list = instance.list_namenodes(request.GET['clusterName'])
    # 获取哪个namenode的resourcemanager是active状态
    active_rm = ''
    res1 = requests.get("http://" + namenode_list[0]['hostIP'] + ":8088" + "/ws/v1/cluster/info")
    res2 = requests.get("http://" + namenode_list[1]['hostIP'] + ":8088" + "/ws/v1/cluster/info")
    res1 = json.loads(res1.text)
    res2 = json.loads(res2.text)
    if res1['clusterInfo']['haState'] == 'ACTIVE':
        active_rm = namenode_list[0]['hostIP']
    if res2['clusterInfo']['haState'] == 'ACTIVE':
        active_rm = namenode_list[1]['hostIP']
    # 获取历史任务的数据
    metrics = []
    for job in jobs:
        # 查询历史指标时将job_id中的application替换为job
        job_id = re.sub('application', 'job', job['job_id'])
        # 请求history server接口访问数据信息，包括物理内存，虚拟内存，CPU执行时间，（虚拟内存比物理内存多一个交换空间内存量）
        url1 = "http://192.168.112.101:19888/ws/v1/history/mapreduce/jobs/" \
              + job_id \
              + "/counters"
        response1 = requests.get(url1)
        job_data1 = json.loads(response1.text)
        # 取出数据
        tmp_obj = {}
        tmp_obj['physicalMemory'] = job_data1['jobCounters']['counterGroup'][2]['counter'][8]['totalCounterValue']
        tmp_obj['virtualMemory'] = job_data1['jobCounters']['counterGroup'][2]['counter'][9]['totalCounterValue']
        tmp_obj['cpuMilliSeconds'] = job_data1['jobCounters']['counterGroup'][2]['counter'][7]['totalCounterValue']
        # 请求active resource manager接口访问数据信息，包括任务状态，任务名称，用户，池，聚合内存，聚合虚拟核心，任务启动时间
        url2 = "http://" + active_rm + ":8088" + "/ws/v1/cluster/apps/" \
               + job['job_id']
        response2 = requests.get(url2)
        job_data2 = json.loads(response2.text)
        tmp_obj['state'] = job_data2['app']['finalStatus']
        tmp_obj['taskName'] = job_data2['app']['name']
        tmp_obj['user'] = job_data2['app']['user']
        tmp_obj['queue'] = job_data2['app']['queue']
        tmp_obj['aggregateMemory'] = job_data2['app']['memorySeconds']
        tmp_obj['aggregateVcore'] = job_data2['app']['vcoreSeconds']
        tmp_obj['launchTime'] = job_data2['app']['launchTime']
        metrics.append({job['job_id']: tmp_obj})
    return Response(metrics)

# @api_view(['POST'])
# def sqoop_import(request):
#     """通过sqoop导入数据
#     """
#     host_ip = request.POST['hostIP']
#     # 拼接sqoop命令字符串
#     command = "import\n" \
#               + "--connect\n" \
#               + "jdbc:" + request.POST['dbType'] + "://" + host_ip + ":3306/" + request.POST['dbName'] + "\n" \
#               + "--username\n" \
#               + request.POST['username'] + "\n" \
#               + "--password\n" \
#               + request.POST['password'] + "\n" \
#               + "--table\n" \
#               + request.POST['tableName'] + "\n" \
#               + "--mapreduce-job-name\n" \
#               + request.POST['jobName'] + "\n" \
#               + "--target-dir\n" \
#               + request.POST['targetDir'] + "\n" \
#               + "--m\n" \
#               + request.POST['mapNums']
#     # 获取libpath路径
#     url = "http://" + host_ip + ":11000" + "/oozie/v2/admin/update_sharelib"
#     response = requests.get(url)
#     lib_path = json.loads(response.text)['sharelibUpdate']['sharelibDirNew'] + "/sqoop"
#     # 将路径中的hdfs:nameservice1替换为hdfs:localhost:8020
#     pattern = r'hdfs://nameservice[0-9]*/'
#     lib_path = re.sub(pattern, "hdfs://" + host_ip + ":8020/", lib_path)
#     # 构建xml文件的参数字典
#     xml_params = {
#         "fs.default.name": "",
#         "mapred.job.tracker": "",
#         "user.name": "",
#         "oozie.sqoop.command": "",
#         "oozie.libpath": "",
#     }
#     xml_params['fs.default.name'] = "hdfs://" + host_ip + ":8020"
#     xml_params['mapred.job.tracker'] = host_ip + ":8032"
#     xml_params['user.name'] = request.POST['sysUser']
#     xml_params['oozie.sqoop.command'] = command
#     xml_params['oozie.libpath'] = lib_path
#     # 获取当文件所在文件夹的绝对路径
#     current_file_path = os.path.dirname(os.path.abspath(__file__))
#     # 拼接配置文件的路径，并将\替换为/
#     resource_file_path = os.path.join(current_file_path, 'resources/config.xml').replace('\\', '/')
#     xml_parse_cli = ParserConf(resource_file_path)
#     # 修改文件内容并写入
#     xml_parse_cli.change_node_text(**xml_params)
#     xml_parse_cli.write_xml(resource_file_path)
#     # 发送请求提交任务
#     headers = {'Content-Type': 'text/xml'}
#     with open(resource_file_path) as xml:
#         r = requests.post('http://192.168.112.101:11000/oozie/v1/jobs?jobtype=sqoop', data=xml)
#         print(r)
#     return Response(xml_params)
