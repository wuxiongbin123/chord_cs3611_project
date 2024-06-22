import hashlib
import os
import sys
from random import choice, sample
import concurrent.futures
import time
import threading
import pydotplus
from PIL import Image
import pyfiglet


from Node import Node
from Network import Network
################################################################################################################


# 计算操作时间并打印的函数
def time_elapsed(start_time, mess):
    # 计算操作所花费的时间
    elapsed_time = time.time() - start_time
    # 打印操作信息以及所花费的时间
    print(f'\n---{mess}耗时：{elapsed_time}秒---')



# 显示菜单并执行用户选择的操作的函数
def show_menu(chord_net, node_ids):
    # 持续显示菜单，直到用户选择退出
    while True:
        # 启动网络的手指表修复任务
        chord_net.periodic_fix()
        # 打印菜单选项
        print('================================================')
        print('1.向网络中插入新节点')
        print('2.在网络中查找数据')
        print('3.向网络中插入数据')
        print('4.打印网络图形')
        print('5.打印网络信息')
        print('6.从网络中删除节点')
        print('7.在网络中查找数据（显示路径）')
        print('8.退出')  # 新增选项
        print('================================================')

        # 获取用户的选择
        choice = input('选择一个操作：')
        print('\n')

        # 处理用户的选择
        if choice == '1':
            # 插入新节点到网络
            node_id = int(input('[->]输入节点ID：'))
            if node_id not in node_ids:
                start_time = time.time()

                chord_net.insert_node(node_id)
                node_ids.append(node_id)

                time_elapsed(start_time, '插入节点')
            else:
                print('[-]节点已存在于网络中。')

        elif choice == '2':
            # 在网络中查找数据
            query = input('[->]搜索数据：')
            start_time = time.time()

            chord_net.find_data(query)
            time_elapsed(start_time, '搜索数据')

        elif choice == '3':
            # 向网络中插入数据
            query = input('[->]输入数据：')
            start_time = time.time()

            chord_net.insert_data(query)

            time_elapsed(start_time, '插入数据')

        elif choice == '4':
            # 打印网络图形
            if len(chord_net.nodes) > 0:
                chord_net.print_network()

        elif choice == '5':
            # 打印网络统计信息
            print(chord_net)

        elif choice == '6':
            # 从网络中删除节点
            node_id = int(input('[->]输入要删除的节点ID：'))
            node_ids.remove(node_id)

            start_time = time.time()

            chord_net.delete_node(node_id)

            time_elapsed(start_time, '删除节点')

        elif choice == '7':
            # 在网络中查找数据（显示路径）
            query = input('[->]搜索数据并显示路径：')
            start_time = time.time()

            chord_net.find_data_with_path(query)
            time_elapsed(start_time, '搜索数据并显示路径')

        elif choice == '8':
            # 退出程序
            sys.exit(0)

        print('\n')


# 创建Chord网络的函数
def create_network():
    # 设置递归限制，以避免深度递归导致的栈溢出
    sys.setrecursionlimit(10000000)

    # 使用pyfiglet库打印ASCII艺术
    ascii_banner = pyfiglet.figlet_format('CHORD')
    print(ascii_banner)
    print('开发人员: 吴雄斌, 李彤基, 万钧杰, 张济昊')
    print('---------------------------------------------')

    # 获取用户输入的m参数
    m_par = int(input('请输入m参数：'))
    Node.m = m_par
    Node.ring_size = 2 ** m_par

    # 打印创建的网络总容量
    print(f'正在创建一个总容量为 {Node.ring_size} 个节点的网络。')
    # 获取用户输入的节点数量
    num_nodes = int(input('请输入节点数量：'))
    # 检查节点数量是否超过网络容量
    while num_nodes > Node.ring_size:
        print('[-]节点数量不能大于网络容量。')
        num_nodes = int(input('请输入节点数量：'))

    # 获取用户输入的假数据数量
    num_data = int(input('请输入要插入的假数据数量：'))

    # 打印创建网络的信息
    print('--------------------------------------------')

    # 生成节点ID列表
    node_ids = list(range(num_nodes))

    # 创建网络对象
    chord_net = Network(m_par, node_ids)

    # 记录开始时间
    start_time = time.time()

    # 使用多进程执行器并行创建节点
    with concurrent.futures.ProcessPoolExecutor() as executor:
        created_nodes = executor.map(chord_net.create_node, node_ids, chunksize=100)
        for node in created_nodes:
            chord_net.nodes.append(node)

    # 将节点插入操作分为两个线程执行
    half = len(chord_net.nodes) // 2
    t1 = threading.Thread(target=chord_net.insert_nodes, args=(chord_net.nodes[:half],))
    t2 = threading.Thread(target=chord_net.insert_nodes, args=(chord_net.nodes[half:],))

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    # 修复所有节点的 fingers table
    # chord_net.fix_network_fingers()

    # 计算创建网络所花费的时间
    time_elapsed(start_time, '网络创建完成')

    # 如果用户选择了插入假数据，执行插入操作
    if num_data > 0:
        chord_net.generate_fake_data(num_data)

    # 显示菜单，让用户选择操作
    show_menu(chord_net, node_ids)


if __name__ == '__main__':
    create_network()
