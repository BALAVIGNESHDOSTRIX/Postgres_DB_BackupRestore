import os 

class FileSizeManager:
    
    #Convert the bytes to target file size(MB.... GB... etc)
    def getFileSize(self,bytes):
        for x in ['bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']:
            if bytes < 1024.0:
                return "%3.1f %s" % (bytes, x)
            bytes /= 1024.0

    #Get the File Information 
    def getFileInspector(self,file_path):
        file_info = os.stat(file_path)
        return True,file_info 

    #Parse the File Attributes like (size,name,date)
    def getInfoParser(self,flag,file_info):
        val = False
        if file_info:
            val = {
                "Size": file_info.st_size,
            }.get(flag)
            if val:
                return True,val
        return False,False 
