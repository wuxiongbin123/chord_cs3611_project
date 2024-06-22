
class Node(object):    #创建Node类
    m = 0
    ring_size = 2 ** m
    def __init__(self, node_id, m):
        self.node_id = node_id
        self.predecessor = self
        self.successor = self
        self.data = dict()
        self.fingers_table = [self]*m

    def __str__(self):
        return f'Node {self.node_id}'

    def __lt__(self, other):
        return self.node_id < other.node_id

    def print_fingers_table(self):
        # 打印当前节点ID、其后继节点ID以及前驱节点ID
        print(f'节点: {self.node_id} 的后继节点: {self.successor.node_id} 和前驱节点: {self.predecessor.node_id}')
        
        # 打印手指表头部信息
        print('手指表:')
        
        # 遍历手指表，并打印每个条目的起始位置和指向的节点ID
        for i in range(self.m):
            start = (self.node_id + 2 ** i) % self.ring_size
            target_node_id = self.fingers_table[i].node_id
            print(f'{start} : {target_node_id}')



    # 将新节点加入网络
    def join(self, node):
        # 寻找网络中给定节点的后继节点
        succ_node = node.find_successor(self.node_id)

        # 寻找后继节点的前驱节点
        pred_node = succ_node.predecessor

        # 在网络中找到合适的插入位置并插入新节点
        self.find_node_place(pred_node, succ_node)

        # 修复新插入节点的手指表
        self.fix_fingers()

        # 将新节点的后继节点的键值对迁移到新节点
        self.take_successor_keys()


    def leave(self):
        #在离开前修复前驱和后继节点的指针
        self.predecessor.successor = self.successor
        self.predecessor.fingers_table[0] = self.successor
        self.successor.predecessor = self.predecessor

         # 将本节点的键值对传递给后继节点
        for key in sorted(self.data.keys()):
            self.successor.data[key] = self.data[key]


    # 在网络中找到新节点的适当位置并插入
    def find_node_place(self, pred_node, succ_node):
        # 更新前驱节点的手指表，使其指向新节点
        pred_node.fingers_table[0] = self
        # 更新前驱节点的后继节点为新节点
        pred_node.successor = self

        # 更新后继节点的前驱节点为新节点
        succ_node.predecessor = self

        # 设置新节点手指表的第一个条目指向其后继节点
        self.fingers_table[0] = succ_node
        # 设置新节点的后继节点
        self.successor = succ_node
        # 设置新节点的前驱节点
        self.predecessor = pred_node

    # 将后继节点中属于当前节点的键值对迁移到当前节点
    def take_successor_keys(self):
        # 从后继节点获取所有属于当前节点的键值对（键小于等于当前节点ID）
        keys_to_transfer = [key for key in self.successor.data if key <= self.node_id]
        for key in keys_to_transfer:
            self.data[key] = self.successor.data[key]

        # 从后继节点中删除已经迁移的键值对
        for key in keys_to_transfer:
            del self.successor.data[key]


    # 更新手指表中的条目
    def fix_fingers(self):
        # 遍历手指表的每个条目
        for i in range(len(self.fingers_table)):
            # 寻找当前节点ID加上2的i次幂的节点后继
            finger_successor = self.find_successor(self.node_id + 2 ** i)
            
            # 更新手指表中的第i个条目
            self.fingers_table[i] = finger_successor


    # 返回离给定哈希键最近的先行节点
    def closest_preceding_node(self, node, hashed_key):
        # 从手指表的最后一个条目开始向前遍历
        for i in range(len(node.fingers_table) - 1, -1, -1):
            # 获取当前手指表条目的节点ID
            current_finger_node_id = node.fingers_table[i].node_id
            
            # 检查当前手指表条目的节点ID是否在查询节点ID和哈希键之间
            # 即检查距离当前手指表条目的节点ID到哈希键的距离是否小于查询节点ID到哈希键的距离
            if self.distance(current_finger_node_id, hashed_key) < self.distance(node.node_id, hashed_key):
                # 如果是，则返回这个节点
                return node.fingers_table[i]

        # 如果没有找到更近的节点，则返回查询节点自身
        return node

    # 计算两个节点ID之间的顺时针距离
    def distance(self, n1, n2):
        # 如果n1小于等于n2，直接计算它们之间的距离
        if n1 <= n2:
            return n2 - n1
        # 如果n1大于n2，计算它们之间的距离，考虑到环的尺寸
        else:
            return self.ring_size - n1 + n2

    # 查找负责给定键的节点
    def find_successor(self, key):
        # 如果当前节点就是键的负责节点，返回当前节点
        if self.node_id == key:
            return self

        # 如果当前节点的后继节点更接近键，返回后继节点
        if self.distance(self.node_id, key) <= self.distance(self.successor.node_id, key):
            return self.successor
        else:
            # 否则，找到离键最近的先行节点，并递归查找键的后继节点
            return self.closest_preceding_node(self, key).find_successor(key)

    # 查找负责给定键的节点，并记录查找路径
    def find_successor_with_path(self, key, path=None):
        # 如果路径列表为空，初始化路径
        if path is None:
            path = []
        # 将当前节点添加到路径中
        path.append(self.node_id)

        # 如果当前节点就是键的负责节点，返回当前节点和路径
        if self.node_id == key:
            return self, path

        # 如果当前节点的后继节点更接近键，将后继节点添加到路径中并返回
        if self.distance(self.node_id, key) <= self.distance(self.successor.node_id, key):
            path.append(self.successor.node_id)
            return self.successor, path
        else:
            # 否则，找到离键最近的先行节点，并递归查找键的后继节点和路径
            next_node = self.closest_preceding_node(self, key)
            return next_node.find_successor_with_path(key, path)

