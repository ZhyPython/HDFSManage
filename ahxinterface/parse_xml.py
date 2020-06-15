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
            if prop_node[0].text in kwargs.keys():
                prop_node[1].text = kwargs[prop_node[0].text]


if __name__ == "__main__":
    # 获取当前文件所在文件夹的绝对路径
    path = os.path.dirname(os.path.abspath(__file__))
    # print(os.path.abspath(__file__))
    # print(path)
    # 拼接配置文件的路径，并将\替换为/
    path = os.path.join(path, 'resources/config.xml').replace('\\', '/')
    # ins = ParserConf(path)
    # xml_params = {
    #     "oozie.wf.application.path": "${nameNode}/workspaces/" \
    #                                  + "taskID_",
    #     "user.name": "root",
    #     "nameNode": "hdfs://" + ":8020",
    #     "resourceManager": ":8032",
    #     "mapperClass": "org.apache.oozie.example.SampleMapper",
    #     "reducerClass": "org.apache.oozie.example.SampleReducer",
    #     "inputDir": "test/input",
    #     "outputDir": "user/${user.name}/output/" \
    #                  + "taskID_",
    # }
    # ins.change_node_text(**xml_params)
    # ins.write_xml("out.xml")
