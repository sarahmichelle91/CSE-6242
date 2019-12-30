from util import entropy, information_gain, partition_classes
import numpy as np 
import ast

class DecisionTree(object):

    def __init__(self):
        # Initializing the tree as an empty dictionary or list, as preferred
        #self.tree = []
        self.tree = {}
        pass

    def learn(self, X, y):
        # TODO: Train the decision tree (self.tree) using the the sample X and labels y
        # You will have to make use of the functions in utils.py to train the tree
        
        # One possible way of implementing the tree:
        #    Each node in self.tree could be in the form of a dictionary:
        #       https://docs.python.org/2/library/stdtypes.html#mapping-types-dict
        #    For example, a non-leaf node with two children can have a 'left' key and  a 
        #    'right' key. You can add more keys which might help in classification
        #    (eg. split attribute and split value)
        self.tree = self.buildTree(X,y)
    
    def buildTree(self, X, y):
        self.tree['depth'] = self.tree.get('depth', 0)
        n0 = 0
        for i in range(len(y)):
            if y[i] == 0: n0 += 1
        n1 = len(y)-n0
        
        ### terminate conditions
        if self.tree['depth'] >= 50:
            if n0 >= n1: return 0
            else: return 1
        if n0 == 0:  return 1
        if n1 == 0: return 0
        
        ### find the best split, only consider numeric attributes
        featureIndex = -1
        splitValue = -1
        featureNum = len(X[0])
        currInfoGain = 0
        for index in range(featureNum): 
            split_val = np.mean([float(i) for i in np.asarray(X)[:,index]])
            X_left, X_right, y_left, y_right = partition_classes(X, y, index, split_val)
            temp = information_gain(y, [y_left, y_right])
            if temp > currInfoGain:
                featureIndex = index
                splitValue = split_val
                currInfoGain = temp
        X_left, X_right, y_left, y_right = partition_classes(X, y, featureIndex, splitValue)
        if len(y_left) == 0 or len(y_right) == 0:
            if n0 >= n1: return 0
            else: return 1
        else:
            self.tree['depth'] += 1
            tempTree = {}
            tempTree[featureIndex] = [splitValue, self.buildTree(X_left, y_left), self.buildTree(X_right, y_right)]
            return tempTree
        
    def classify(self, record):
        # TODO: classify the record using self.tree and return the predicted label
        temp = self.tree
        while isinstance(temp, dict):
#            print(list(temp.keys()))
            featureIndex = list(temp.keys())[0]
#            print(featureIndex, record[featureIndex], self.tree[featureIndex][0], self.tree['depth'])
#            print(self.tree[featureIndex][1], self.tree[featureIndex][2])
#            print(self.tree)
            if record[featureIndex] <= temp[featureIndex][0]: temp = temp[featureIndex][1]
            else: temp = temp[featureIndex][2]
        return temp