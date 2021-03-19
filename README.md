# Project 1: POSIX DFS With Probabilistic Routing

See the project spec here: https://www.cs.usfca.edu/~mmalensek/cs677/assignments/project-1.html  

[Design Document ](https://docs.google.com/document/d/1FKjiulu46H7eGEc4WfyfaS5pCzWCWl6xxE26PTfOhks/edit?usp=sharing)

Author: JiaChen and Jerry  

## Configuration

**IMPORTANT**
In utils/Constant.java, you must specify the FILEPATH_PREFIX to /bigdata/${whoami} if you want to deploy this project on orions.  

## Entrance
java -cp dfs.jar edu.usfca.cs.dfs.DFSApp

## Start server
```
server [port for clients to connect] [port for nodes to connect]
```
## start Client 
```
client [server's ip] [server's port]
```
## start storage node 
```
storage [server's ip] [server's port] [node name(could be any)] [node port for client to connect]
```
## start posix-client 
```
fuse [server's ip] [server's port] [mount point]
```

## Example of running the project

### server(supposed running on orion01)
Orion01 will run the dfs server, 8080 is for client to connect, 9090 is for nodes
```
java -jar dfs.jar server 8080 9090
```
### nodes 
A Node will connect to orion01:9090, and register a name as node1, 9098 is for other nodes and client to connect  
```
java -jar dfs.jar storage orion01 9090 node1 9098  
```
Other nodes could run the same command

### client
```
5. java -jar dfs.jar client orion01 8080 // the client wil connect to orion01:8080 and run a few commands like: 
```
After running the client, you can use some arguments:
* -nodes 
  * look up active nodes
* -save [local file] [cluster directory] 
  * upload the local file to cluster. the [cluster directory] must be a directory path. 
  * exapmle: -save /bigdata/mchen81/afile.txt /aDir/
* -retrieve [cluster file path] [local file path] 
  * retrieve the file on cluster, must to provide a new file name for [local file path]
  * exapmle -retrieve /aDir/afile.txt /bigdata/mchen81/aNewFile.txt
* -ls [cluster path] 
  * list only directories on the given path


### Posix-client
```
5. java -jar dfs.jar fuse orion01 8080 /bigdata/mchen81/dfsFuse // Note: /bigdata/mchen81/dfsFuse must exist before run this
```
After running the posix, you may check out files on cluster through /bigdata/mchen81/dfsFuse  


