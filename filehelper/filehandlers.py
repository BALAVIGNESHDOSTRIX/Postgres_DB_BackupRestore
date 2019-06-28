import os

class FileHandler:

    #Method for checking the if file is exsisting or not in the required directory
    def isFileAvail(self,path):
        if os.path.isfile(path):
            return True 
        return False

    #Method for Get All the files inside in the required directory
    def ListDirFiles(self,path):
        return os.listdir(path)

    #Get the filename of the given Index
    def GetFileIndexName(self,path, choice):
        files = self.ListDirFiles(path)
        i = 1
        for f in files:
            if i == choice:
                return f 
            i = i + 1
