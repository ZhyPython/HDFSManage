# -*- coding: UTF-8 -*-
import paramiko
import re

class SSHClient:
    """ssh连接远程虚拟机并执行命令
    """
    def __init__(self, hostname, port="22", username="root", password="123"):
        """实例化ssh客户端,创建默认的白名单
        """
        self.ssh = paramiko.SSHClient()
        self.policy = paramiko.AutoAddPolicy()
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password

    def connect(self):
        """设置白名单,连接服务器，
        设置服务器的ip，服务器的端口，服务器的用户名，用户名对应的密码
        """
        self.ssh.set_missing_host_key_policy(self.policy)
        self.ssh.connect(
            hostname=self.hostname,
            port=self.port,
            username=self.username,
            password=self.password
        )

    def exec_download_cmd(self, hdfs_file, host_path):
        """远程执行命令
        exec_command 返回的对象都是类文件对象
        command为命令字符串，如 "ls"
        stdin 标准输入 用于向远程服务器提交参数，通常用write方法提交
        stdout 标准输出 服务器执行命令成功，返回的结果  通常用read方法查看
        stderr 标准错误 服务器执行命令错误返回的错误值  通常也用read方法
        """
        self.connect()
        command = "hdfs dfs -get" + " " + hdfs_file + " " + host_path
        stdin, stdout, stderr = self.ssh.exec_command(command)
        # 返回的为字节，解码为字符串
        result = stdout.read().decode()
        error = stderr.read().decode()
        # print("结果", result)
        # print("错误", error)
        self.ssh.close()
        return result, error

    def test_directory(self, path):
        """判断路径是否存在
        """
        self.connect()
        command = "ls" + path
        stdin, stdout, stderr = self.ssh.exec_command(command)
        if stdout.readline() != '':
            pass
        else:
            # 不存在时创建目录
            result = self.ssh.exec_command("mkdir -p" + " " + path)

    def sqoop_pro(
        self,
        IP,
        db_type,
        connect_type,
        connect_name,
        db_host_ip,
        database,
        username,
        password,
        table,
        job_name,
        target_dir,
        map_nums,
        sys_user
    ):
        """将数据从MySQL导入到hdfs中
        IP: 执行命令的主机IP，通常为namemode
        db_type: 数据库类型，MySQL，SQL server，Oracle...
        connect_type: Oracle的连接方式
        connect_name: Oracle的连接名称
        db_host_ip: 数据库所在主机的IP地址
        database: 数据库名
        username: 远程连接数据库的用户名
        password: 远程连接数据库的用户名对应的密码
        table: 数据库中的表名
        job_name: 设置提交的mapreduce任务名
        target_dir: 输出文件在hdfs上的目录，必须为不存在的目录，不然会报错
        map_nums: map的任务数量，决定着输出文件part-m-*的数量
        sys_user: 提交任务的用户
        """
        # 根据数据库类型判断command
        command = ''
        if db_type == 'mysql':
            command = "sqoop import" \
                    + " --connect jdbc:" + db_type + "://" + db_host_ip + ":3306/" + database \
                    + " --username " + username \
                    + " --password " + password \
                    + " --table " + table \
                    + " --mapreduce-job-name " + job_name \
                    + " --target-dir " + target_dir \
                    + " --m " + map_nums
        if db_type == 'sqlserver':
            command = "sqoop import" \
                    + " --driver com.microsoft.sqlserver.jdbc.SQLServerDriver" \
                    + " --connect \"jdbc:" + db_type + "://" + db_host_ip + ":1433;database=" + database + "\"" \
                    + " --username " + username \
                    + " --password " + password \
                    + " --table " + table \
                    + " --mapreduce-job-name " + job_name \
                    + " --target-dir " + target_dir \
                    + " --m " + map_nums
        if db_type == 'oracle':
            symbol = "/" if connect_type == 'serviceName' else ":"
            command = "sqoop import" \
                    + " --connect jdbc:oracle:thin:@" + db_host_ip + ":1521" + symbol + connect_name \
                    + " --username " + username \
                    + " --password " + password \
                    + " --table " + database + '.' + table \
                    + " --mapreduce-job-name " + job_name \
                    + " --target-dir " + target_dir \
                    + " --m " + map_nums
        
        print(command)
        self.connect()
        context = {
            'jobId': '',
            'error': ''
        }
        try:
            # 检测sys_user是否在linux的用户列表中，不存在则创建
            res = self.ssh.exec_command(
                "cat /etc/passwd |awk -F \: '{print $1}' | grep " + sys_user
            )
            if res[1].read().decode() == "":
                _ = self.ssh.exec_command("useradd " + sys_user)
            # 用指定的用户执行sqoop任务的命令
            command = "sudo -u " + sys_user + " " + command
            stdin, stdout, stderr = self.ssh.exec_command(command, get_pty=True)
            # 输出stdout,获取任务id
            pattern = r'INFO mapreduce.Job: Running job: (.*)'
            pattern_err = r'ERROR manager.SqlManager: Error executing statement: java.sql.SQLException: (.*)'
            for line in stdout:
                # print(line.strip('\n'))
                # 匹配字符
                if re.search(pattern, line):
                    job_id = re.search(pattern, line).group(1).strip()
                    # 将job_id中的job替换为application
                    context['jobId'] = re.sub('job', 'application', job_id)
                    return context
                # 匹配报错信息
                if re.search(pattern_err, line):
                    context_err = re.search(pattern_err, line).group(1).strip()
                    context['error'] = context_err
                    return context
        except Exception as e:
            context['error'] = e
        finally:
            self.ssh.close()
        return context


if __name__ == "__main__":
    CLI = SSHClient("192.168.112.101")
    # CLI.exec_download_cmd("/test.xls", "~")
    # CLI.test_directory('/download_hdfs')
    # CLI.sqoop_pro('mysql', '192.168.112.102', 'test', 'root', '123456', 'runoob_tbl', 'sqoop', '/data', '1', 'test')
