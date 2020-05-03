# -*- coding:utf-8 -*-
import os
from xml.etree.ElementTree import parse, Element


class ParserConf:
    def __init__(self, path):
        """读取路径中的文件，赋初值
        """
        self.tree = parse(path)

    def write_xml(self, out_path):
        """将修改后的配置重新输出
        """
        self.tree.write(out_path, encoding="utf-8", xml_declaration=True)

    def change_node_text(self, **kwargs):
        """改变xml文件中的text内容
        """
        root = self.tree.getroot()
        for prop_node in root:
            if prop_node[0].text == "fs.default.name":
                prop_node[1].text = kwargs['fs.default.name']
            if prop_node[0].text == "mapred.job.tracker":
                prop_node[1].text = kwargs['mapred.job.tracker']
            if prop_node[0].text == "user.name":
                prop_node[1].text = kwargs['user.name']
            if prop_node[0].text == "oozie.sqoop.command":
                prop_node[1].text = kwargs['oozie.sqoop.command']
            if prop_node[0].text == "oozie.libpath":
                prop_node[1].text = kwargs['oozie.libpath']


if __name__ == "__main__":
    # 获取当文件所在文件夹的绝对路径
    path = os.path.dirname(os.path.abspath(__file__))
    # 拼接配置文件的路径，并将\替换为/
    path = os.path.join(path, 'resources/config.xml').replace('\\', '/')
    ins = ParserConf(path)
    ins.change_node_text()
    ins.write_xml("out.xml")
