import numpy as np

from _Gutils._PathsAndVariables import *

class JDF:
    def __init__(self):
        self.npyAr=None
        self.ndf=None
        self.columns=None
        self.shape=None
        self.icolumns=None

    def getcolfilename(self, filePath):
        if ".npy" not in filePath:
            print("need to be given a numpy file for JDF to find the columns")
            print('use JDF.help() for more')
            quit(20)
        return filePath[0:filePath.find(".npy")] + ".txt"

    def getCols(self, filePath):
        # get where the .npy is and replace it with .txt to get the column file
        # path if it exists
        colfile = self.getcolfilename(filePath)
        print('col file: {}'.format(colfile))
        with open(colfile, 'r') as fd:
            lines = fd.readlines()
            self.columns = lines[0].strip("\n").split(" ")
            for i in range(len(self.columns)):
                self.columns[i] = self.columns[i].strip()
        return self.columns

    # load a numpy array for analysis
    def loadJDF_file(self, filePath,columns=None, genDF=False):
        self.nAr = np.load(filePath)

    def storeJDF_file(self, filePath, npr=None, columns=(), verbose=False):
        if npr is not None:
            self.npyAr = npr
        np.save(filePath, self.npyAr)
        if len(columns) == self.npyAr.shape[1]:
            colfile = self.getcolfilename(filePath)
            print("the column file: {}".format(colfile))
            with open(colfile, 'w') as fp:
                for c in columns:
                    fp.write(c + " ")
                fp.write('\n')




jnsyDF = JDF()

testnpy = np.empty([2, 2], dtype=float)

print(testnpy)
jnsyDF.storeJDF_file(data_path + r"\test.npy", npr=testnpy, columns=['a', 'b'])