import hashlib
import os
import sys
from random import choice, sample
import concurrent.futures
import time
import threading
import pydotplus

from PIL import Image
from Node import Node


# 自定义网络异常类
class NetworkError(Exception):
    def __init__(self, msg='[-]Network Error!', *args, **kwargs):
        # 调用基类的初始化方法，并传递错误消息和其他参数
        super().__init__(msg, *args, **kwargs)


class Network:
    # 初始化网络类
    def __init__(self, m, node_ids):
        # 初始化节点列表
        self.nodes = []
        # 设置哈希环的大小为2的m次幂
        self.m = m
        self.ring_size = 2 ** m
        # 插入第一个节点
        self.insert_first_node(node_ids[0])
        # 记录第一个节点
        self.first_node = self.nodes[0]
        # 从节点ID列表中移除第一个节点ID
        node_ids.pop(0)


    # 定义对象的字符串表示
    def __str__(self):
        # 返回网络的字符串表示，包括存活节点数、总容量、参数m和第一个插入的节点
        return (f'Chord网络:\n'
                f' |存活节点: {len(self.nodes)} 个。\n'
                f' |总容量: {self.ring_size} 个节点。\n'
                f' |参数 m: {self.m}。\n'
                f' |第一个插入的节点: {self.first_node.node_id}。\n')


    # 打印网络中所有节点的信息
    def print_network(self):
        # 遍历网络中的每个节点
        for node in self.nodes:
            # 打印当前节点的手指表
            node.print_fingers_table()
            # 打印当前节点的数据
            print(f"节点 {node.node_id} 的数据:")
            print(node.data)
            print("\n")  # 添加空行以提高可读性


    # 修复网络中所有节点的手指表
    def fix_network_fingers(self):
        # 从第一个节点开始修复网络的手指表
        self.first_node.fix_fingers()

        # 初始化当前节点为第一个节点的手指表的第一项
        current_node = self.first_node.fingers_table[0]

        # 当当前节点不是第一个节点时，继续修复手指表
        while current_node != self.first_node:
            # 修复当前节点的手指表
            current_node.fix_fingers()
            # 更新当前节点为其手指表的第一项
            current_node = current_node.fingers_table[0]



    # 定义哈希函数，将键哈希到一个指定长度的标识符
    def hash_function(self, key):
        # 获取节点哈希比特数
        num_bits = self.m

        # 将键转换为字节串并进行哈希
        hashed_bytes = hashlib.sha1(str.encode(key)).digest()

        # 计算所需字节数以表示哈希标识符
        required_bytes = (num_bits + 7) // 8

        # 将哈希字节转换为整数
        # 'big' 表示最高有效字节位于字节数组的开头
        hashed_id = int.from_bytes(hashed_bytes[:required_bytes], 'big')

        # 如果哈希比特数不是8的倍数，调整哈希标识符
        if num_bits % 8:
            hashed_id >>= 8 - num_bits % 8

        # 返回哈希标识符
        return hashed_id

    # 创建新节点的函数
    def create_node(self, node_id):
        # 创建一个新的节点对象
        node = Node(node_id, self.m)
        # 将新节点添加到网络中的节点列表
        return node

    # 插入多个节点的函数
    def insert_nodes(self, nodes):
        for node in nodes:
            try:
                if node.node_id > self.ring_size:
                    raise NetworkError('[-]节点ID应小于或等于网络大小。')
                # 将新节点加入网络
                print(f'[+]节点 {node.node_id} 通过节点 {self.first_node.node_id} 加入网络。')

                node.join(self.first_node)
            except NetworkError as e:
                print(e)

    # 插入单个节点的函数
    def insert_node(self, node_id):
        try:
            if node_id > self.ring_size:
                raise NetworkError('[-]节点ID应小于或等于网络大小。')

            # 创建新节点并添加到网络中的节点列表
            self.nodes.append(self.create_node(node_id))

            node = self.nodes[-1]

            # 将新节点加入网络
            print(f'[+]节点 {node.node_id} 通过节点 {self.first_node.node_id} 加入网络。')

            node.join(self.first_node)

            # self.fix_network_fingers()  # 注释掉的部分暂时不执行
        except NetworkError as e:
            print(e)


    # 从网络中删除指定节点的函数
    def delete_node(self, node_id):
        try:
            # 过滤网络中的节点，找到与指定ID匹配的节点
            node = list(filter(lambda temp_node: temp_node.node_id == node_id, self.nodes))[0]
        except IndexError:
            # 如果找不到节点，打印错误信息
            print(f'[-]节点 {node_id} 未找到！')
        else:
            # 调用节点的离开方法
            node.leave()
            # 从网络的节点列表中移除该节点
            self.nodes.remove(node)
            # 修复网络的手指表
            self.fix_network_fingers()

    # 插入第一个节点的函数
    def insert_first_node(self, node_id):
        # 打印初始化网络和插入第一个节点的信息
        print(f'[!]初始化网络，正在插入第一个节点 {node_id}。')
        # 创建一个新的节点对象
        node = Node(node_id, self.m)
        # 将新节点添加到网络中的节点列表
        self.nodes.append(node)


    # 在网络中查找数据的函数
    def find_data(self, data):
        # 计算数据的哈希键
        hashed_key = self.hash_function(data)
        # 打印查找数据的哈希键
        print(f'[*]正在查找 \'{data}\'，键为 {hashed_key}')
        # 初始化当前节点为网络中的第一个节点
        node = self.first_node
        # 调用当前节点的查找后继节点方法
        node = node.find_successor(hashed_key)
        # 查找数据
        found_data = node.data.get(hashed_key, None)
        # 如果找到数据，打印找到的数据和对应的节点ID
        if found_data is not None:
            print(f'[+]在节点 {node.node_id} 中找到 \'{data}\'，键为 {hashed_key}')
            print()
        # 如果没有找到数据，打印错误信息
        else:
            print(f'[-]\'{data}\' 在网络中不存在')

    # 在网络中查找数据并返回路径的函数
    def find_data_with_path(self, data):
        # 计算数据的哈希键
        hashed_key = self.hash_function(data)
        # 打印查找数据的哈希键
        print(f'[*]正在查找 \'{data}\'，键为 {hashed_key}')
        # 初始化当前节点为网络中的第一个节点
        node = self.first_node
        # 调用当前节点的查找后继节点方法，并返回路径
        node, path = node.find_successor_with_path(hashed_key)
        # 查找数据
        found_data = node.data.get(hashed_key, None)
        # 如果找到数据，打印找到的数据和对应的节点ID，以及路径
        if found_data is not None:
            print(f'[+]在节点 {node.node_id} 中找到 \'{data}\'，键为 {hashed_key}')
            print(f'路径: {" -> ".join(map(str, path))}')
            print()
        # 如果没有找到数据，打印错误信息
        else:
            print(f'[-]\'{data}\' 在网络中不存在')

    # 在网络中插入数据的函数
    def insert_data(self, key):
        node = self.first_node
        # 计算数据的哈希键
        hashed_key = self.hash_function(key)
        # 打印保存数据的哈希键和对应的节点ID
        print(f'[+]保存键：{key}，哈希：{hashed_key} -> 节点：{node.find_successor(hashed_key).node_id}')
        # 找到数据的负责节点
        succ = node.find_successor(hashed_key)
        # 在负责节点上插入数据
        succ.data[hashed_key] = key

    # 生成假数据的函数
    def generate_fake_data(self, num):
        # 定义文件扩展名列表
        extensions = ['.txt', '.png', '.doc', '.mov', '.jpg', '.py']
        # 生成假文件名列表
        files = [f'file_{i}' + choice(extensions) for i in range(num)]
        # 定义开始时间
        start_time = time.time()
        # 遍历假文件名列表，调用插入数据的函数
        for temp in files:
            self.insert_data(temp)
        # 打印生成数据的时间信息
        print(f'\n {float(time.time() - start_time) / num} 秒 ---')



    # 打印网络并创建图形表示的函数
    def print_network(self):
        # 打开文件用于写入网络图形信息
        with open('graph.dot', 'w+') as f:
            # 写入图形的开始标签
            f.write('digraph G {\r\n')
            # 遍历网络中的每个节点
            for node in self.nodes:
                # 创建一个数据字符串，包含节点的键值对信息
                data = 'Keys:\n-------------\n'
                for key in sorted(node.data.keys()):
                    data += f'key: {key} - data: \'{node.data[key]}\'\n'
                # 创建一个手指表字符串，包含节点的fingers table信息
                fingers = 'Finger Table:\n-------------\n'
                for i in range(self.m):
                    fingers += f'{(node.node_id + 2 ** i) % self.ring_size} : {node.fingers_table[i].node_id}\n'
                # 如果数据字符串不为空，写入节点信息
                if data != '':
                    # 写入数据标签和连接
                    f.write(f'data_{node.node_id} [label=\"{data}\", shape=box]\r\n')
                    f.write(f'{node.node_id}->data_{node.node_id}\r\n')
                # 如果手指表字符串不为空，写入手指表标签和连接
                if fingers != '':
                    f.write(f'fingers_{node.node_id} [label=\"{fingers}\", shape=box]\r\n')
                    f.write(f'{node.node_id}->fingers_{node.node_id}\r\n')
            # 写入图形的结束标签
            f.write('}')

        # 尝试使用pydotplus库将图形转换为png文件并显示
        try:
            graph_a = pydotplus.graph_from_dot_file('graph.dot')
            graph_a.write_png('graph.png', prog='circo')
            graph_image = Image.open('graph.png')
            graph_image.show()
        except pydotplus.graphviz.InvocationException:
            pass

    # 启动定时任务来定期修复网络的手指表的函数
    def periodic_fix(self):
        # 创建一个定时器，每15秒执行一次修复手指表的函数
        threading.Timer(15, self.fix_network_fingers).start()



################################################################################################################