# -*- coding: UTF-8 -*-

import sys

from hdfs.client import Client, InsecureClient
from hdfs.util import HdfsError


"""
对于特殊权限，需要在最前面增加一位，SUID:4,SGID:2,SBIT:1
chmod 4777 test --test拥有SUID权限，rwsrwxrwx
chmod 2777 test --test拥有SGID权限，rwxrwsrwx
chmod 1777 test --test拥有SBIT权限，rwxrwxrwt
"""

class HDFSClient:
    def __init__(self, url, root=None, user=None, proxy=None, timeout=None, session=None):    
        """ 连接hdfs
        url: HDFS名称节点的主机名或IP地址及端口号
        root: 根路径，此路径将作为传递给客户端的所有HDFS路径的前缀
        user: 使用InsecureClient（Base Client）,指定访问hdfs的用户;Client使用默认用户dr.who
        proxy: 代理的用户
        timeout: 连接超时，转发到请求处理程序
        session: request.Session实例，用于发出所有请求
        """
        if user:
            self.client = InsecureClient(url, user=user)
        else:
            self.client = Client(url, root=root, proxy=proxy, timeout=timeout, session=session)

    def list_hdfs_file(self, hdfs_path, status=False):
        """ 返回目录下的文件
        status: 每个文件或目录的属性信息(FileStatus)
        return: 列表中包含元组，每个元组是目录名或文件名和属性信息构成
        """
        return self.client.list(hdfs_path, status=status)
    
    def walk_hdfs_file(self, hdfs_path, depth=0, status=False, ignore_missing=False, allow_dir_changes=False):
        """ 深度遍历远程文件系统
        hdfs_path: 起始路径。如果该路径不存在，则会引发HdfsError。如果指向文件，则返回的生成器将为空
        depth: 探索的最大深度。0为无限制
        status: 同时返回每个文件或文件夹的相应FileStatus
        ignore_missing: 忽略缺少的嵌套文件夹，而不是引发异常
        allow_dir_changes: 允许更改目录列表以影响遍历
        return: 生成器，返回值参考python的walk函数
        """
        return self.client.walk(hdfs_path, depth=depth, status=status, ignore_missing=ignore_missing, 
                                allow_dir_changes=allow_dir_changes)

    def delete_hdfs_file(self, hdfs_path, recursive=False, skip_trash=False):
        """ 删除文件
        recursive: 递归删除文件或目录，默认情况下，如果尝试删除非空目录，此方法将引发HdfsError
        skip_trash: 设置为false时，已删除的路径将被移动到适当的垃圾回收文件夹，而不是被删除
        return: 如果删除成功，则此函数返回True；如果hdfs_path之前不存在文件或目录，则返回False
        """
        return self.client.delete(hdfs_path, recursive=recursive, skip_trash=skip_trash)
    
    def download_hdfs_file(self, hdfs_path, local_path, overwrite=True, n_threads=1, temp_dir=None, **kwargs):
        """ 下载文件
        hdfs_file: HDFS上要下载的文件或文件夹的路径。如果是文件夹，则将下载该文件夹下的所有文件
        local_file: 本地路径。如果它已经存在并且是目录，则文件将在其中下载
        overwrite: 覆盖任何现有文件或目录
        n_threads: 用于并行化的线程数。值为0（或负数）将使用与文件一样多的线程
        temp_dir: 当overwrite = True并且最终目标路径已经存在时，将首先在其下下载文件的目录。下载成功完成后，它将被交换
        **kwargs: 关键字参数转发给read()。如果未传递chunk_size参数，则将使用默认值64 kB
        return: 方法执行成功，将返回本地下载路径
        """
        res = self.client.download(hdfs_path, local_path, overwrite=overwrite, n_threads=n_threads, 
                                   temp_dir=temp_dir, **kwargs)

    def upload_hdfs_file(self, hdfs_path, local_path, n_threads=1, temp_dir=None, chunk_size=65536, progress=None, 
                         cleanup=True, **kwargs):
        """ 上传文件
        hdfs_path: 目标HDFS路径。如果它已经存在并且是目录，则文件将在其中上传
        local_path: 文件或文件夹的本地路径。如果是文件夹，则将上载其中的所有
            文件（请注意，这意味着没有文件的文件夹将不会远程创建）
        cleanup: 如果上传过程中发生错误，删除所有上传的文件
        return: 方法执行成功，将返回状态码，远程上传目录，错误信息
        """
        try:
            res = self.client.upload(hdfs_path, local_path, n_threads=n_threads, temp_dir=temp_dir, chunk_size=chunk_size,
                                     progress=progress, cleanup=cleanup, overwrite=True)
            return 0, res, ''
        except HdfsError as e:
            return 1, '', str(e)
    
    def makedirs(self, hdfs_path, permission=None):
        """ 创建目录，可以递归
        permission: 在新创建的目录上设置的八进制权限，这些权限将仅在尚不存在的目录上设置
        return: None
        """
        self.client.makedirs(hdfs_path, permission=permission)

    def parts(self, hdfs_path, parts=None, status=False):
        """
        hdfs_path: 远程路径。该目录每个分区最多应包含一个零件文件（否则将任意选择一个文件）
        parts: 零件文件编号列表或要选择的零件文件总数。如果是数字，那么将随机选择那么多分区。 
               默认情况下，将返回所有零件文件。如果部件是列表，但未找到部件之一或需要太多样本，则会引发HdfsError
        status: 返回文件的FileStatus
        return: 返回对应于路径的零件文件的字典
        """
        return self.client.parts(hdfs_path, parts=parts, status=status)
    
    def read_hdfs_file(self, **kwds):
        """ 读取文件内容，这个方法必须在一个with块中使用，以便关闭连接
        >>> with client.read('foo') as reader:
        >>>     content = reader.read()
        hdfs_path: HDFS路径
        offset: 起始字节位置
        length: 要处理的字节数。设置为None时会读取整个文件
        buffer_size: 用于传输数据的缓冲区大小（以字节为单位）。默认为在HDFS配置中设置的值
        encoding: 用于解码请求的编码。默认情况下，返回原始数据
        chunk_size: 如果设置为正数，则上下文管理器将返回一个生成器，该生成器生成每个chunk_size字节，
                而不是类似文件的对象（除非还设置了定界符）
        delimiter: 如果设置，上下文管理器将在每次遇到定界符时返回生成器。此参数要求指定编码
        progress: 回调函数，用于跟踪进度，称为每个chunk_size字节（如果未指定块大小，则不可用）。
                它将传递两个参数，即要上传的文件的路径和到目前为止已传输的字节数。
                完成后，将以-1作为第二个参数调用一次
        """
        self.client.read(**kwds)
    
    def write_hdfs_file(self, hdfs_path, data=None, overwrite=False, permission=None, 
                        blocksize=None, replication=None, buffersize=None, append=False, 
                        encoding=None):
        """ 在HDFS上创建文件
        data: 要写入的文件内容。 可以是字符串，生成器或文件对象。 最后两个选项将允许流式上传（即无需
              将全部内容加载到内存中）。 如果为None，则此方法将返回类似文件的对象，应使用with块调用
              它（请参见下面的示例）
        permission: 在新创建的文件上设置的八进制权限
        append: 附加到文件而不是创建新文件
        encoding: 用于序列化写入数据的编码
        >>> from json import dump, dumps
        >>> records = [
        >>>     {'name': 'foo', 'weight': 1},
        >>>     {'name': 'bar', 'weight': 2},
        >>> ]
        >>> # As a context manager:
        >>> with client.write('data/records.jsonl', encoding='utf-8') as writer:
        >>>     dump(records, writer)
        >>> Or, passing in a generator directly:
        >>> client.write('data/records.jsonl', data=dumps(records), encoding='utf-8')
        """ 
        self.client.write(hdfs_path, data=data, overwrite=overwrite, permission=permission,
                          blocksize=blocksize, replication=replication, buffersize=buffersize,
                          append=append, encoding=encoding)

    def rename_or_move(self, hdfs_src_path, hdfs_dst_path):
        """ 移动文件或目录
        hdfs_src_path: 源路径
        hdfs_dst_path: 目标路径，如果路径已经存在并且是目录，则源将移入其中。
                如果路径存在并且是文件，或者缺少父目标目录，则此方法将引发HdfsError
        """
        self.client.rename(hdfs_src_path, hdfs_dst_path)
    
    def set_owner(self, hdfs_path, owner=None, group=None):
        """ 更改文件的所有者，必须至少指定所有者和组之一
        owner: 可选，文件的新所有者
        group: 可选，文件的新所有组
        """
        self.client.set_owner(hdfs_path, owner=owner, group=group)
    
    def set_permission(self, hdfs_path, permission):
        """ 更改文件权限
        permission: 文件的新八进制权限字符串
        """
        self.client.set_permission(hdfs_path, permission)
    
    def set_replication(self, hdfs_path, replication):
        """ 设置文件副本
        replication: 副本数
        """
        self.client.set_replication(hdfs_path, replication)

    def set_times(self, hdfs_path, access_time=None, modification_time=None):
        """ 更改文件的时间戳
        """
        self.client.set_times(hdfs_path, access_time=access_time, modification_time=modification_time)

    def status_hdfs_file(self, hdfs_path, strict=True):
        """ 获取文件的FileStatus
        strict: 如果为False，则返回None，而不是如果路径不存在则引发异常
        """
        self.client.status(hdfs_path, strict=strict)



if __name__ == "__main__":
    cli = HDFSClient("http://192.168.112.101:9870", user='hdfs')

    # 列出当前路径下的目录
    dir = cli.list_hdfs_file('/', status=True)
    print(dir)

    # 循环列出目录
    # dir_list = cli.walk_hdfs_file('/', depth=0)
    # for d in dir_list:
    #     print(d)
    
    # 创建目录
    # cli.makedirs('/test', permission="755")

    # 删除目录
    # flag = cli.delete_hdfs_file('/test', recursive=False, skip_trash=True)
    # print(flag)

    # 重命名目录
    # cli.rename_or_move('/test', '/test1')

    # 更改所有者
    # cli.set_owner('/test', owner='hdfs')

    # 上传文件
    # 若无法上传，可能是本地无法访问集群内网IP，将集群所有IP与主机名的映射都加入host文件中即可
    # cli.upload_hdfs_file('/test1/', 'd:\\test.xls')

    # 下载文件
    # cli.download_hdfs_file('/test/axis.log', 'e:\\axis.log')

