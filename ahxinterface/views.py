import json

from rest_framework.response import Response
from rest_framework.decorators import api_view
from .cm_info import CMInfo
from .hdfs_client import HDFSClient


# Create your views here.
@api_view(['PUT'])
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

@api_view(['PUT'])
def invoke_agent(request):
    """向计算代理服务器发送计算信息
    接收的json串为：
        {"taskID": "", "algorithm": "", "inputVariableList": [], "isGpu": "false"}
        输入列表中的对象为：
            {"variableName": "", "variableHDPath": ""}
    返回的信息：
        {"taskID": "", "status": , "errorMsg": "", "outputVariableList": []}
        输出列表中的对象为：
            {"variableName": "", "variableHDPath": ""}
    """
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
