# -*- coding: UTF-8 -*-
from datetime import datetime
import threading
import time
import json

from django.conf import settings
from pprint import pprint
import cm_client
import pytz


class HDFSMonitor:
    """单例模式，加锁保证线程安全;用双重检查保证单例的实现
    """
    _instance_lock = threading.Lock()

    # API连接属性
    cm_client.configuration.username = settings.CM_USERNAME
    cm_client.configuration.password = settings.CM_PASSWORD
    _api_host = settings.CM_API_HOST
    _port = settings.CM_PORT
    _api_version = settings.CM_API_VERSION

    # 查询指标时的query语句,扩展指标时，只需添加 “指标名：元组” ，元组中为显示名称、
    # 查询语句和单位
    _query_dic = {
        "dfs_capacity":
            (
                "配置的HDFS容量",
                "SELECT dfs_capacity "\
                "WHERE clusterName = '{}' AND category = SERVICE",
                "GB"
            ),
        "dfs_capacity_used_non_hdfs":
            (
                "使用的非 HDFS",
                "SELECT dfs_capacity_used_non_hdfs "\
                "WHERE clusterName = '{}' AND category = SERVICE",
                "GB",
            ),
        "dfs_capacity_used":
            (
                "使用的HDFS容量",
                "SELECT dfs_capacity_used "\
                "WHERE clusterName = '{}' AND category = SERVICE",
                "GB"
            ),
        "total_bytes_read_rate_across_datanodes":
            (
                "各 DataNodes 中的总读取的字节",
                "SELECT total_bytes_read_rate_across_datanodes "\
                "WHERE clusterName = '{}' AND category = SERVICE",
                "Bytes/second",
            ),
        "total_bytes_written_rate_across_datanodes":
            (
                "各 DataNodes 中的总写入的字节",
                "SELECT total_bytes_written_rate_across_datanodes "\
                "WHERE clusterName = '{}' AND category = SERVICE",
                "Bytes/second",
            ),
        "total_blocks_read_rate_across_datanodes":
            (
                "各 DataNodes 中的总读取块",
                "SELECT total_blocks_read_rate_across_datanodes "\
                "WHERE clusterName = '{}' AND category = SERVICE",
                "blocks/second",
            ),
        "total_blocks_written_rate_across_datanodes":
            (
                "各 DataNodes 中的总已写入块",
                "SELECT total_blocks_written_rate_across_datanodes "\
                "WHERE clusterName = '{}' AND category = SERVICE",
                "blocks/second",
            ),
        "packet_ack_round_trip_time_nanos_avg_time_across_datanodes":
            (
                "整个 DataNodes 中的数据包确认往返的平均时间",
                "SELECT packet_ack_round_trip_time_nanos_avg_time_across_datanodes "\
                "WHERE clusterName = '{}' AND category = SERVICE",
                "nanos",
            ),
        "send_data_packet_transfer_nanos_avg_time_across_datanodes":
            (
                "整个 DataNodes 中的发送数据包传输的平均时间",
                "SELECT send_data_packet_transfer_nanos_avg_time_across_datanodes "\
                "WHERE clusterName = '{}' AND category = SERVICE",
                "us",
            ),
        "send_data_packet_blocked_on_network_nanos_avg_time_across_datanodes":
            (
                "整个 DataNodes 中的发送网络阻止数据包的平均时间",
                "SELECT send_data_packet_blocked_on_network_nanos_avg_time_across_datanodes "\
                "WHERE clusterName = '{}' AND category = SERVICE",
                "us",
            ),
        "flush_nanos_rate_across_datanodes":
            (
                "整个 DataNodes 中的磁盘刷新",
                "SELECT flush_nanos_rate_across_datanodes "\
                "WHERE clusterName = '{}' AND category = SERVICE",
                "operations/second",
            ),
        "flush_nanos_avg_time_across_datanodes":
            (
                "整个 DataNodes 中的平均磁盘刷新时间",
                "SELECT flush_nanos_avg_time_across_datanodes "\
                "WHERE clusterName = '{}' AND category = SERVICE",
                "us",
            ),
    }

    def __init__(self):
        pass

    def __new__(cls, *args, **kwargs):
        if not hasattr(HDFSMonitor, "_instance"):
            with HDFSMonitor._instance_lock:
                if not hasattr(HDFSMonitor, "_instance"):
                    # 创建实例
                    HDFSMonitor._instance = object.__new__(cls)
                    # 初始化连接
                    HDFSMonitor._instance.init_conn(cls._api_host, cls._port, cls._api_version)
        return HDFSMonitor._instance

    def init_conn(self, host, port, version):
        """初始化连接，构建cm的连接实例
        """
        url = host + ':' + port + '/api/' + version
        client = cm_client.ApiClient(url)
        # 生成资源API
        self._cluster_api_instance = cm_client.ClustersResourceApi(client)
        self._time_series_api_instance = cm_client.TimeSeriesResourceApi(client)
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

    def list_namenodes(self, cluster_name):
        """获取namenodes
        """
        # 获取hdfs服务相关属性
        services = self._services_api_instance.read_services(cluster_name)
        # print(services)
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
        return namenode_list

    # def list_datanodes(self, cluster_name):
    #     """获取集群中的datanodes
    #     """
    #     api_response = self._cluster_api_instance.list_hosts(cluster_name)
    #     # pprint(api_response)
    #     return api_response

    def hdfs_metrics(self, key, cluster_name):
        """获取指标值
        key为get请求传递的参数，是具体的指标名称
        from_time设定从1个小时前统计
        desired_rollup设定返回的指标值为1分钟一个
        """
        from_time = datetime.fromtimestamp(time.time() - 7200)
        # to_time = datetime.fromtimestamp(time.time())
        desired_rollup = 'RAW'
        query = HDFSMonitor._query_dic.get(key)[1].format(cluster_name)
        result = self._time_series_api_instance.query_time_series(_from=from_time,
                                                                  query=query,
                                                                  desired_rollup=desired_rollup,
                                                                  must_use_desired_rollup=False)
        ts_list = result.items[0]
        # pprint(ts_list)
        # 存储指标数据的字典
        metrics = {}
        # 将显示名称存入metircs
        metrics['displayName'] = HDFSMonitor._query_dic.get(key)[0]
        # 将指标数据的单位存入metrics
        metrics['unit'] = HDFSMonitor._query_dic.get(key)[2]
        # for ts in ts_list.time_series:
            # print(
            #     "--- %s: %s ---" %
            #     (ts.metadata.attributes['entityName'], ts.metadata.metric_name)
            # )
        # time_series中的第一个元素为要获取的指标值
        ts = ts_list.time_series[0]
        metrics['entityName'] = ts.metadata.attributes['entityName']
        metrics['metricName'] = ts.metadata.metric_name
        # print("开始时间 %s" % self.utc_to_local(ts.metadata.start_time))
        # 存储指标值的列表
        temp_timestamp = []
        temp_value = []
        for point in ts.data:
            # print("%s:\t%s" % (point.timestamp, point.value))
            temp_timestamp.append(point.timestamp)
            # 将value转化为对应单位的值
            if metrics['unit'] == "GB":
                temp_value.append(point.value / 1024 / 1024 / 1024)
            elif metrics['unit'] == "us":
                temp_value.append(point.value / 1024)
            else:
                temp_value.append(point.value)
        metrics['timestamp'] = temp_timestamp
        metrics['value'] = temp_value
        # 序列化指标数据
        json_metrics = json.dumps(metrics, ensure_ascii=False)
        # print(json_metrics)
        return json_metrics

    def utc_to_local(self, utc_time_str, utc_format='%Y-%m-%dT%H:%M:%S.%fZ'):
        """将utc转化为北京时间
        """
        local_tz = pytz.timezone('Asia/Shanghai')
        local_format = "%Y-%m-%d %H:%M:%S"
        utc_dt = datetime.strptime(utc_time_str, utc_format)
        local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
        time_str = local_dt.strftime(local_format)
        return datetime.fromtimestamp(int(time.mktime(time.strptime(time_str, local_format))))
    
    # def sqoop(self):
    #     try:
    #         res = self._services_api_instance.create_sqoop_user_dir_command(
    #             'Cluster 1',
    #             'sqoop_client'
    #         )
    #         pprint(res)
    #     except cm_client.rest.ApiException as e:
    #         print(e)



if __name__ == "__main__":
    instance = HDFSMonitor()
    clusters = instance.list_clusters()
    # instance.hdfs_metrics('dfs_capacity_used_non_hdfs')
    # instance.list_namenodes('Cluster 1')
    # instance.sqoop()
