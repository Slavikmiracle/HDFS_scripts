import requests
import os
import subprocess

PWD = " "

def ls():
    data={'user.name':'root', 'op':'LISTSTATUS'}
    path = input("Enter path(`user`)")
    url = f'http://localhost:50070/webhdfs/v1/{path}'
    response = requests.get(url, params=data)
    print(response.content)

def mkdir():
    path = input("Enter fullpath in hadoop: ")
    url = f'http://localhost:50070/webhdfs/v1/user/{path}'
    data = {'user.name':'root','op':'MKDIRS'}
    print(url)
    response = requests.put(url,params=data)
    print(response)
    print(response.content)

def put():
    data = {'op':'CREATE', 'user.name':'root','namenoderpcaddress':'localhost:8089', 'createparent':'true','overwrite':'true'}
    path=input("Enter path in hadoop: ")
    url = f'http://slavik:50075/webhdfs/v1/{path}'
    pathInLinux = input("Enter path in Linux: ")
    fp = open(pathInLinux, 'rb')
    files={'file':fp}
    response = requests.put(url, files=files, params=data)
    print(response)
    print(response.content)

def rm():
    data = {'op':"DELETE", 'user.name':'root'}
    path = input("Enter path in hadoop:  ")
    url = f'http://localhost:50070/webhdfs/v1/{path}'
    response = requests.delete(url, params=data)
    print(response)
    print(response.content)
#curl -i -X PUT -T ~slavik/mysources/WordCount/file02 "http://slavik:50075/webhdfs/v1/tmp/file01?op=CREATE&user.name=root&\
#namenoderpcaddress=localhost:8089&createflag=&createparent=true&overwrite=true"
#    print(response.content)
#curl -i -X PUT "http://localhost:50070/webhdfs/v1/user/dir1?\
#user.name=root&op=MKDIRS" 



#ls()
#mkdir()
#put()

def showDir():
    print('Files in this directory')
    subprocess.call(["ls","-l", "-a", PWD])
#    listDir = os.listdir(path='.')
#    for a in listDir:
#        print(a, "   ")

def cd(str):
    global PWD
    if str[0] == '/':
        PWD = str
    else:
        PWD = PWD + '/' + str


def main():
    process= subprocess.Popen(["pwd"], stdout=subprocess.PIPE)
    global PWD
    PWD = process.communicate()[0].decode('utf-8').rstrip()
#    command = input('Enter command - ')
#    print(command.split())
    showDir()
    cd("/home")
    showDir()
#    ls()
#    mkdir()
#    rm()

main()
