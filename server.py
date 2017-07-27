import requests, time
from requests import Session
import xlrd
import requests
import json
import os


localhost = "http://localhost:8001/files/temp/"
s = requests.Session()


print (s.get(localhost).text)


dir = "/Users/nellylyu/workspace/PythonModule/dummy_file"

def getAllFilesInTree(dirPath):
    print("hitting")
    _files = []
    for folder, subfolders, files in os.walk(dirPath):
        print(files)
        for _file in files:
            filePath = os.path.join(os.path.abspath(folder), _file)
            _files.append(filePath)
    #print(os.path.abspath(folder))

#print(_files)
    return _files


def getAllTempFileRecords():
    response = requests.get(localhost)
    records = response.json()
    _files = [i["path"] for i in records]
    return _files

def checkTempForUnlogged(tempDir):
    tempInDB = getAllTempFileRecords()
    tempInFolder = getAllFilesInTree(tempDir)
    unloggedFiles =[_file for _file in tempInFolder if _file not in tempInDB]
    
    return unloggedFiles

def logNewFile(_file):
    filename = os.path.basename(_file)
    path = _file
    expired = 0
    complete = 0
    
    request = requests.post(localhost + "new/", data = {'filename':filename,'path':path,'expired':expired,'complete':complete})
    #print (request)
    return request



def main():
    filesToLog = checkTempForUnlogged(dir)
    if not filesToLog:
        return "Nothing to Log"
    for _file in filesToLog:
        print(_file)
        logNewFile(_file)
    
    return "Output this to txt file for reporting"


if __name__ == "__main__":
    main()
