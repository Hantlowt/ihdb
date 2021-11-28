import uuid
import os
from glob import glob


class Node:
    def __init__(self, node_id, node_category, node_data, node_relations, ihdb, save=True):
        self.id = node_id
        self.category = node_category
        self.relations = node_relations
        self.__ihdb__ = ihdb
        if save:
            self.data = {}
            for key in node_data.keys():
                self.__setitem__(key, node_data[key])
        else:
            self.data = node_data

    def __getitem__(self, index):
        result = self.data.get(index)
        if result is None:
            result = self.get_relations(index)
        return result

    def __setitem__(self, key, value):
        if type(value) == Node or (type(value) == list and all(type(n) == Node for n in value)):
            self.add_relation(key, value)
        else:
            is_index = self.__ihdb__.index_exist(self.category, key)
            if is_index:
                self.__ihdb__.delete_index_node(self, key)
            self.data[key] = value
            self.save()
            if is_index:
                self.__ihdb__.add_index_node(self, key)

    def __iter__(self):
        for key in self.data.keys():
            yield key, self.data[key]

    def save(self, category_has_changed=False):
        if category_has_changed:
            self.__ihdb__.delete(self)
        self.__ihdb__.save(self)

    def add_relation(self, name, node):
        if type(node) == list:
            self.relations[name] = []
            for n in node:
                self.relations[name] += [n.category + ':' + n.id]
        else:
            self.relations[name] = [node.category + ':' + node.id]
        self.save()

    def delete_relation(self, name, node=None):
        if node is not None:
            self.relations[name] = [n for n in self.relations[name] if n != node.category+':'+node.id]
        else:
            self.relations.pop(name)
        self.save()

    def get_relation(self, name):
        return self.get_relations(name)[0]

    def get_relations(self, name):
        relations = self.relations.get(name)
        if relations is None:
            return []
        result = []
        for r in relations:
            r = r.split(':')
            r_id = r[1]
            r_cat = r[0]
            result += [self.__ihdb__.get_node_from_id(r_id, r_cat)]
        return result


class Ihdb:
    def __init__(self, folder_path="db"):
        self.folder_path = folder_path
        if not os.path.isdir(folder_path):
            os.mkdir(folder_path)

    def index_exist(self, category, key):
        return os.path.isdir(self.folder_path + '/' + category + '/' + key)

    def delete_index_node(self, node, key):
        folder_path = self.folder_path + '/' + node.category + '/' + key
        obj = node[key]
        if obj is not None:
            file_path = folder_path + '/' + str(obj)
            file = open(file_path, 'r')
            content = file.read()
            content = content.replace(str(node.id) + '\n', '')
            file.close()
            if content == '':
                os.remove(file_path)
            else:
                file = open(file_path, 'w')
                file.write(content)
                file.close()

    def add_index_node(self, node, key):
        folder_path = self.folder_path + '/' + node.category + '/' + key
        obj = node[key]
        if obj is not None:
            file_path = folder_path + '/' + str(obj)
            file = open(file_path, 'a' if os.path.isfile(file_path) else 'w')
            file.write(str(node.id) + '\n')
            file.close()

    def add_index(self, category, key):
        if not self.index_exist(category, key):
            os.mkdir(self.folder_path + '/' + category + '/' + key)
        for node in self.nodes(category):
            self.add_index_node(node, key)

    def read_node_from_file(self, path):
        f = open(path, 'r')
        path = path.replace('\\', '/')
        path = path.split('/')
        content = f.read()
        content = content.split('\n')
        node = Node(path[-1], path[-2], eval(content[0]), eval(content[1]), self, save=False)
        f.close()
        return node

    def get_all_categories(self):
        return [x for x in os.listdir(self.folder_path) if os.path.isdir(self.folder_path+'/'+x)]

    def get_nodes_from_category(self, category='node', where=None):
        folder = self.folder_path + '/' + category
        result = []
        key = where.split(' ')[0] if where is not None else None
        index_exist = False if where is None else self.index_exist(category, key)
        if where is None or not index_exist:
            for n in glob(folder + '/*'):
                if os.path.isfile(n):
                    result += [self.read_node_from_file(n)]
        if where is not None:
            if index_exist:
                condition = where.replace(key, '(int(value) if value.isnumeric() else value)')
                values = [os.path.basename(value) for value in glob(folder + '/' + key + '/*')]
                values = [value for value in values if eval(condition)]
                for v in values:
                    f = open(folder + '/' + key + '/' + v)
                    content = f.read().split('\n')
                    result += [self.node(i, category) for i in content]
                    f.close()
            else:
                condition = where.replace(key, f"node['{key}']")
                result = [node for node in result if eval(condition)]
        return list(filter(None,result))

    def nodes(self, category='node', select=None, where=None):
        if select is not None:
            if type(select) == list:
                result = [{k: r[k] for k in list(r.data.keys()) + list(r.relations.keys()) if k in select} for r in
                          self.get_nodes_from_category(category, where)]
            else:
                result = [r[select] for r in self.get_nodes_from_category(category, where)]
        else:
            result = self.get_nodes_from_category(category, where)
        return result

    def node(self, node_id, node_category):
        return self.get_node_from_id(node_id, node_category)

    def get_node_from_id(self, node_id, node_category):
        location = self.folder_path + '/' + node_category
        if os.path.isfile(location + '/' + node_id):
            return self.read_node_from_file(location + '/' + node_id)
        return None

    def delete(self, node):
        os.remove(self.folder_path + '/' + node.category + '/' + node.id)
        if not os.listdir(self.folder_path + '/' + node.category):
            os.rmdir(self.folder_path + '/' + node.category)

    def save(self, node):
        location = self.folder_path + '/' + node.category
        if not os.path.isdir(location):
            os.mkdir(location)
        f = open(location + '/' + node.id, 'w')
        f.write(str(node.data) + '\n')
        f.write(str(node.relations))
        f.close()

    def create_node(self, node_category='node', node_data=None):
        n = Node(uuid.uuid4().hex, node_category, {} if node_data is None else node_data, {}, self)
        return n
