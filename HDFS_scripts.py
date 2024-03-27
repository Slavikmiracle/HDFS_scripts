import requests
import os
import subprocess

PWD = " "
PWD_HDFS = "user"


# Вывести список каталогов HDFS
def ls():
    data={'user.name':'root', 'op':'LISTSTATUS'}
    global PWD_HDFS
    url = f'http://localhost:50070/webhdfs/v1/{PWD_HDFS}'
    response = requests.get(url, params=data)
    print(response.content)


#Создать папку в HDFS
def mkdir(path):
    if path[0] == '/':
        url = f'http://localhost:50070/webhdfs/v1{path}'
    else:
         url = f'http://localhost:50070/webhdfs/v1/{PWD_HDFS}/{path}'
    data = {'user.name':'root','op':'MKDIRS'}
    print(url)
    response = requests.put(url,params=data)
    print(response)
    print(response.content)


# Изменение директории в HDFS
def chDir(str):
    global PWD_HDFS
    if str[0] == '/':
        PWD_HDFS = str
    else:
        PWD_HDFS = PWD_HDFS + '/' + str
    print(PWD_HDFS)


# Загрузка файла в HDFS(доделать)
def put(path1, path2):
    data = {'op':'CREATE', 'user.name':'root','namenoderpcaddress':'localhost:8089', 'createparent':'true','overwrite':'true'}
#    path=input("Enter name in hadoop: ")
    if path1[0] == '/':
        url = f'http://slavik:50075/webhdfs/v1{path1}'
    else:
         url = f'http://slavik:50075/webhdfs/v1/{PWD_HDFS}/{path1}'
    if path2[0] == '/':
         pathInLinux = path2
    else:
         pathInLinux = PWD +'/'+ path2
    print(url, pathInLinux)
    fp = open(pathInLinux, 'rb')
    print(url," ", pathInLinux)
    files={'file':fp}
    response = requests.put(url, files=files, params=data)
    print(response)
    print(response.content)


#Удаление файла (доделать)
def rm(path):
    data = {'op':"DELETE", 'user.name':'root', 'recursive':'true'}
    if path[0] == '/':
        url = f'http://localhost:50070/webhdfs/v1/user{path}'
    else:
        url = f'http://localhost:50070/webhdfs/v1/user/{PWD_HDFS}/{path}'
    response = requests.delete(url, params=data)
    print(response)
    print(response.content)

def showDir():
    print('Files in this directory')
    subprocess.call(["ls","-l", "-a", PWD])


def cd(str):
    global PWD
    if str[0] == '/':
        PWD = str
    else:
        PWD = PWD + '/' + str
    print(PWD)


#Конкатенация двух файлов объединяется в path1
def concat(path1, path2):
    data = {'op':'CONCAT', 'user.name':'root','sources':f'{path2}'}
    if path1[0] == '/':
        url = f'http://localhost:50070/webhdfs/v1{path1}'
    else:
        url = f'http://localhost:50070/webhdfs/v1/{PWD_HDFS}/{path1}'
    response = requests.post(url, params=data)
    print(response)
    print(response.content)
 #curl -i -X POST "http://localhost:50070/webhdfs/v1/user/123?op=CONCAT&sources=/user/122&user.name=root"
   
#Доделать
def dowload(path, path1):
    data = {'op':'OPEN', 'user.name':'root','namenoderpcaddress':'localhost:8089'}
    if path[0] == '/':
        url = f'http://localhost:50070/webhdfs/v1{path}'
    else:
        url = f'http://localhost:50070/webhdfs/v1/{PWD_HDFS}/{path}'
    response = requests.get(url, params=data)
    if path1[0] == '/':
         pathInLinux = path1
    else:
         pathInLinux = PWD +'/'+ path1
    with open(pathInLinux, 'wb') as file:
            for chunk in response.iter_content(chunk_size=128):
                file.write(chunk)
 #       print("Файл успешно загружен из HDFS")
    print(response)
    print(response.content)
    subprocess.call(["cat","-l", "-a", PWD])
# Дальше сохраняем
#    curl -i -L "http://slavik:50075/webhdfs/v1/user/123?op=OPEN&user.name=root&namenoderpcaddress=localhost:8089"


def main():
    process= subprocess.Popen(["pwd"], stdout=subprocess.PIPE)
    global PWD
    PWD = process.communicate()[0].decode('utf-8').rstrip()
    while (True):
        command = input('Enter command - ')
        commandArray = command.split()
    #   print(command.split())
        if (commandArray[0] == "lshdfs"):
            ls()
        if (commandArray[0] == "cd"):
            cd(commandArray[1])
        if (commandArray[0] == "ls"):
            showDir()
        if (commandArray[0] == "cdhdfs"):
            chDir(commandArray[1])
        if (commandArray[0] == "rm"):
            rm(commandArray[1])
        if (commandArray[0] == "put"):
            put(commandArray[1],commandArray[2])
        if (commandArray[0] == "concat"):
            concat(commandArray[1],commandArray[2])
        if (commandArray[0] == "get"):
            dowload(commandArray[1], commandArray[2])
        if (commandArray[0] == "mkdir"):
            mkdir(commandArray[1])
        if (commandArray[0] == "stop"):
            break

main()
