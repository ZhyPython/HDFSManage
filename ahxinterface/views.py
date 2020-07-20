import json
import requests
import os
import re
from datetime import datetime

from rest_framework.response import Response
from rest_framework.decorators import api_view
from .cm_info import CMInfo
from .hdfs_client import HDFSClient
from .parse_xml import ParserConf


# Create your views here.
@api_view(['POST'])
def upload_file(request):
    """将文件上传至集群
    上传文件的信息为:
        {"fileName": "", "filePath": "", "fileHDPath": ""}
    返回每个文件的信息为:
        {"status": , "filePath": "", "fileHDPath": "", "errorMessage": ""} 
    """
    # 上传时每个文件的信息，文件名，本地路径，上传到hdfs的路径
    file_name = request.data["fileName"]
    file_local_path = request.data["filePath"]
    file_hdfs_path = request.data["fileHDPath"]
    # 获取active的namenode
    cm_info_ins = CMInfo()
    activeNN = cm_info_ins.get_activeNN()
    # 连接hdfs
    hdfs_client = HDFSClient("http://" + activeNN['hostIP'] + ":9870")
    # 上传文件
    res = hdfs_client.upload_hdfs_file(file_hdfs_path, file_local_path)
    # 返回信息
    response_message = dict(
        status=res[0],
        filePath=file_local_path,
        fileHDPath=res[1],
        errorMessage=res[2]
    )
    return Response(response_message)

@api_view(['GET'])
def download_file(request):
    """下载文件到客户端
    下载时每个文件的信息为：
        {"fileName": "", "fileHDPath": "", "filePath": ""}
    """
    file_name = request.GET["fileName"]
    file_local_path = request.GET["filePath"]
    file_hdfs_path = request.GET["fileHDPath"]
    # 获取active的namenode
    cm_info_ins = CMInfo()
    activeNN = cm_info_ins.get_activeNN()
    # 连接hdfs
    hdfs_client = HDFSClient("http://" + activeNN['hostIP'] + ":9870")
    # 下载文件
    hdfs_client.download_hdfs_file(file_hdfs_path, file_local_path)
    return Response()

@api_view(['POST'])
def invoke_agent(request):
    """向计算代理服务器发送计算信息
    接收的json串为：
        {"taskID": "", "algorithm": "", "inputVariableList": [], "isGpu": "false"}
        inputVariableList输入列表中的对象为：
            {"variableName": "", "variableHDPath": ""}
    返回的信息：
        {"taskID": "", "status": , "errorMsg": "", "outputVariableList": []}
        outputVariableList输出列表中的对象为：
            {"variableName": "", "variableHDPath": ""}
    """
    # 获取active的namenode的主机名
    cm_info_ins = CMInfo()
    activeNN = cm_info_ins.get_activeNN()
    active_host = activeNN['hostName']
    
    # 获取当前文件所在文件夹的绝对路径
    current_file_path = os.path.dirname(os.path.abspath(__file__))
    # 拼接本地config配置文件的路径，并将\替换为/
    config_file_path = os.path.join(current_file_path, 'resources/config.xml').replace('\\', '/')
    # 拼接本地workflow配置文件的路径
    workflow_file_path = os.path.join(current_file_path, 'resources/workflow.xml').replace('\\', '/')

    # 构建年月日递进目录格式
    date_dir = str(datetime.now().year) + '/' + str(datetime.now().month) + '/' + str(datetime.now().day) + '/'
    # hdfs上的workflow路径，必须和jar包所在lib目录平级
    workflow_hdfs_path = "${nameNode}/workspaces/" \
                         + date_dir \
                         + "taskID_" + request.data['taskID'],
    # 构建xml文件的参数字典，向config.xml文件写入配置
    xml_params = {
        "oozie.wf.application.path": workflow_hdfs_path,
        "user.name": "root",
        "nameNode": "hdfs://" + active_host + ":8020",
        "resourceManager": active_host + ":8032",
        "mapperClass": "org.apache.oozie.example.SampleMapper",
        "reducerClass": "org.apache.oozie.example.SampleReducer",
        "inputDir": "test/input",
        "outputDir": "user/${user.name}/output/" \
                     + date_dir
                     + "taskID_" + request.data['taskID'],
    }
    xml_parse_cli = ParserConf(config_file_path)
    # 修改文件内容并写入
    xml_parse_cli.change_node_text(**xml_params)
    xml_parse_cli.write_xml(config_file_path)

    # 上传workflow.xml文件
    hdfs_client = HDFSClient("http://" + active_host + ":9870")
    _ = hdfs_client.upload_hdfs_file(workflow_hdfs_path, workflow_file_path)

    # 获取config.xml文件的配置内容
    with open(config_file_path, encoding='utf-8') as fp:
        body = fp.read()

    url = "http://" + active_host + ":11000/oozie/v2/jobs?action=start"
    r = requests.post(url, data=body.encode("utf-8"), headers={'Content-Type': 'application/xml'})
    # print(r.text)

    # 获取任务结果并返回,首先获取luancher状态，若为failed，则直接返回failed，再获取实际job状态

    return Response()

def append_path(*strs):
    """将字符串拼接在一起,使得字符串中只有一个'/'
    """
    # 获取传入的参数个数
    nums = len(strs)
    # 拼接完成的字符串,开始时赋值为第一个字符串
    complete_str = strs[0]
    # 遍历元组进行拼接
    for i in range(nums-1):
        # 需要拼接的字符串
        s = strs[i+1]
        # 去除complete_str最后的一个'/'
        l = len(complete_str)
        p = complete_str[0: l-1] if l > 0 and complete_str[l-1] == '/' else complete_str
        # 判断s的第一位是否为 "/"
        s = s[1: len(s)] if s[0] == '/' else s
        complete_str = p + '/' + s
    return complete_str
