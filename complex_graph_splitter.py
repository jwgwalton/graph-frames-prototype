from collections import defaultdict

from graphframes import GraphFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructField, ArrayType, StringType, StructType


def add_opposite_direction_edges(edge_list):
    """
    The shortest path algorithm uses the direction of the edges are directional,
    adding an edge in other direction of equal weight gives the effect of undirected edges.
    """
    inverse_edge_list = edge_list
    inverse_edge_list = inverse_edge_list.withColumnRenamed('src', 'dst_copy')
    inverse_edge_list = inverse_edge_list.withColumnRenamed('dst', 'src')
    inverse_edge_list = inverse_edge_list.withColumnRenamed('dst_copy', 'dst')
    return edge_list.union(inverse_edge_list.select('src', 'dst', 'weight'))


def is_better_path(newpath, oldpath):
    """
    is new path more strongly weighted than old path

    newpath, oldpath:  lists of tuples with node and weight
    """
    new_path_length = [float(node[1]) for node in newpath]
    old_path_length = [float(node[1]) for node in oldpath]

    # This returns the most strongly weighted path rather than shortest, is this a problem for long paths of weak edges?
    return sum(new_path_length) > sum(old_path_length)


def find_closest_ch_path(edge_list, vertex, path=[]):
    # list of tuples, (node_id, path_weight)
    path = path + [vertex]

    if vertex[0].startswith('CH'):
        return path

        # TODO: max path length constraint?

    possible_dsts = edge_list.filter(edge_list['src'] == vertex[0]).rdd.map(lambda r: (r[1], r[2])).collect()
    shortest = None

    for node in possible_dsts:
        if node not in path:
            newpath = find_closest_ch_path(edge_list, node, path)
            if newpath:
                if not shortest or is_better_path(newpath, shortest):
                    shortest = newpath
    return shortest


def split_complex_sub_graphs(vertex_list, edge_list):
    """
    Split sub graph, return a list of the split subgraphs vertex lists

    """
    # graph is undirected so need edges in both directions
    edge_list = add_opposite_direction_edges(edge_list)

    subgraphs_dict = defaultdict(list)

    all_nodes = vertex_list.rdd.map(lambda r: (r[0], 0)).collect()

    for node in all_nodes:
        closest_ch_path = find_closest_ch_path(edge_list, node)
        closest_ch_node = closest_ch_path[-1][0]
        subgraphs_dict[closest_ch_node].append(node[0])

    return [subgraph for subgraph in subgraphs_dict.values()]