# -*- coding: UTF-8 -*-
import json
import threading

from django.conf import settings
from pprint import pprint
import cm_client
import requests


class CMInfo:
    """单例模式，加锁保证线程安全;用双重检查保证单例的实现
    """
    _instance_lock = threading.Lock()

    # API连接属性
    cm_client.configuration.username = settings.CM_USERNAME
    cm_client.configuration.password = settings.CM_PASSWORD
    _api_host = settings.CM_API_HOST
    _port = settings.CM_PORT
    _api_version = settings.CM_API_VERSION

    def __init__(self):
        pass

    def __new__(cls, *args, **kwargs):
        if not hasattr(CMInfo, "_instance"):
            with CMInfo._instance_lock:
                if not hasattr(CMInfo, "_instance"):
                    # 创建实例
                    CMInfo._instance = object.__new__(cls)
                    # 初始化连接
                    CMInfo._instance.init_conn(cls._api_host, cls._port, cls._api_version)
        return CMInfo._instance

    def init_conn(self, host, port, version):
        """初始化连接，构建cm的连接实例
        """
        url = host + ':' + port + '/api/' + version
        client = cm_client.ApiClient(url)
        # 生成资源API
        self._cluster_api_instance = cm_client.ClustersResourceApi(client)
        # 为了获取namemode的资源API
        self._services_api_instance = cm_client.ServicesResourceApi(client)
        self._roles_api_instance = cm_client.RolesResourceApi(client)
        self._host_api_instance = cm_client.HostsResourceApi(client)

    def list_clusters(self):
        """查询集群数量及名称
        """
        api_response = self._cluster_api_instance.read_clusters(view='SUMMARY')
        # print(api_response.items)
        # 获取集群的name和display_name
        cluster_list = []
        for cluster in api_response.items:
            dic = {}
            dic['displayName'] = cluster.display_name
            dic['name'] = cluster.name
            cluster_list.append(dic)
        # pprint(cluster_list)
        # return json.dumps(cluster_list, ensure_ascii=False)
        return cluster_list

    def get_activeNN(self):
        """获取namenodes
        """
        # 先获取集群
        cluster_list = self.list_clusters()
        cluster_name = cluster_list[0]['name']
        # 获取hdfs服务相关属性
        services = self._services_api_instance.read_services(cluster_name)
        hdfs = None
        for service in services.items:
            if service.type == 'HDFS':
                hdfs = service
        # 获取namenode的roles实例
        roles = self._roles_api_instance.read_roles(cluster_name,
                                                    hdfs.name,
                                                    filter='type==NAMENODE',
                                                    view='summary')
        # 获取namenode的ip和名称
        namenode_list = []
        for role in roles.items:
            response = self._host_api_instance.read_host(role.host_ref.host_id)
            dic = {}
            dic['hostName'] = response.hostname
            dic['hostIP'] = response.ip_address
            namenode_list.append(dic)
        # pprint(namenode_list)
        # 获取active的namenode
        activeNN = None
        nn1 = "http://" + namenode_list[0]['hostIP'] + ":" + "9870"
        nn2 = "http://" + namenode_list[1]['hostIP'] + ":" + "9870"
        response1 = requests.get(nn1 + "/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus")
        response2 = requests.get(nn2 + "/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus")
        if json.loads(response1.content)['beans'][0]['State'] == 'active':
            activeNN = namenode_list[0]
        elif json.loads(response2.content)['beans'][0]['State'] == "active":
            activeNN = namenode_list[1]
        else:
            pass
        # print(activeNN)
        return activeNN


if __name__ == "__main__":
    instance = CMInfo()
    # clusters = instance.list_clusters()
    instance.get_activeNN()
