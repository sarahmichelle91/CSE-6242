import http.client
import json
import time
import timeit
import sys
import collections
from pygexf.gexf import *


#
# implement your data retrieval code here
#

### read API key
api_key = sys.argv[1]
#api_key = '821cbc4d374fb2b0488f2307cdaad09f'


### Q1.1.a
if __name__ == "__main__":
    global res_min_parts 
    res_min_parts = 1000
    api = http.client.HTTPConnection('www.rebrickable.com')
    while True:
        start_time = time.time()
        url = 'https://rebrickable.com/api/v3/lego/sets/?key=' + \
               api_key + '&page_size=1000&min_parts=' + str(res_min_parts) + \
               '&ordering=-num_parts'
        api.request('GET', url)
        jsondata = json.loads(api.getresponse().read())
        curr_num = jsondata['count']
        time.sleep(max(0, 1-time.time()+start_time)) ## 1 request per second
        if curr_num <= 300 and curr_num >= 270:
            break
        res_min_parts += 10
    
    global res_sets 
    res_sets = []
    curr_set = jsondata['results']
    for i in range(curr_num):
        res_sets.append(curr_set[i])
    
#    print(min_parts())
#    print(len(res_sets))

### Q1.1.b
    global all_parts
    all_parts = []
    api = http.client.HTTPConnection('www.rebrickable.com')
    for i in range(curr_num):
        start_time = time.time()
        parts = []
        set_num = curr_set[i]['set_num']
        url = 'https://rebrickable.com/api/v3/lego/sets/' + set_num + \
              '/parts/?key=' + api_key + '&page_size=1000'
        api.request('GET', url)
        jsondata = json.loads(api.getresponse().read())
        part_infos = jsondata['results']
        for j in range(min(1000,jsondata['count'])):
            parts.append([part_infos[j]['color']['rgb'], part_infos[j]['quantity'], \
                          part_infos[j]['part']['name'], part_infos[j]['part']['part_num']])
            all_parts.append(sorted(parts,key = lambda x: x[1], reverse= True)[0:20])
            time.sleep(max(0, 1-time.time()+start_time)) ## 1 request per second
    

#    gexf_graph()
    
# complete auto grader functions for Q1.1.b,d
def min_parts():
    """
    Returns an integer value
    """
    # you must replace this with your own value
    return res_min_parts

def lego_sets():
    """
    return a list of lego sets.
    this may be a list of any type of values
    but each value should represent one set

    e.g.,
    biggest_lego_sets = lego_sets()
    print(len(biggest_lego_sets))
    > 280
    e.g., len(my_sets)
    """
    # you must replace this line and return your own list
    return res_sets

def hex_to_rgb(value):
    value = value.lstrip('#')
    lv = len(value)
    return tuple(int(value[i:i + lv // 3], 16) for i in range(0, lv, lv // 3))

def gexf_graph():
    """
    return the completed Gexf graph object
    """
    # you must replace these lines and supply your own graph

    gexf = Gexf("alu64","LEGO")
    graph=gexf.addGraph("undirected","static","bricks_graph")
    attr = graph.addNodeAttribute("Type", type = "string")
    count = 0
    for i in range(len(all_parts)):
        temp = graph.addNode(res_sets[i]['set_num'], res_sets[i]['name'], \
                             r = '0', g = '0', b = '0')
        temp.addAttribute(attr, "set")
        for j in range(len(all_parts[i])):
            this_id = all_parts[i][j][0] + '+' + all_parts[i][j][3]
            this_r, this_g, this_b = hex_to_rgb(all_parts[i][j][0])
            temp = graph.addNode(this_id, all_parts[i][j][2],r = str(this_r), \
                                 g = str(this_g), b = str(this_b))
            temp.addAttribute(attr, "node")
            graph.addEdge(str(count), res_sets[i]['set_num'], this_id, weight = str(all_parts[i][j][1]))
            count += 1

    output_file=open("bricks_graph.gexf","wb")
    gexf.write(output_file)
    output_file.close()
    
    return gexf.graphs[0]

# complete auto-grader functions for Q1.2.d

def avg_node_degree():
    """
    hardcode and return the average node degree
    (run the function called “Average Degree”) within Gephi
    """
    # you must replace this value with the avg node degree
    return 5.541

def graph_diameter():
    """
    hardcode and return the diameter of the graph
    (run the function called “Network Diameter”) within Gephi
    """
    # you must replace this value with the graph diameter
    return 8

def avg_path_length():
    """
    hardcode and return the average path length
    (run the function called “Avg. Path Length”) within Gephi
    :return:
    """
    # you must replace this value with the avg path length
    return 4.408

