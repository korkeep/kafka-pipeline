#### Run Hadoop 🐘
```bash
ubuntu@ubuntu:~/code/bigdata-pipeline/etl$ hdfs namenode -format
ubuntu@ubuntu:~/code/bigdata-pipeline/etl$ start-dfs.sh
ubuntu@ubuntu:~/code/bigdata-pipeline/etl$ start-yarn.sh
```
```bash
ubuntu@ubuntu:~/code/bigdata-pipeline/etl$ jps
11776 SecondaryNameNode
12304 Jps
12162 NodeManager
11411 NameNode
12024 ResourceManager
11546 DataNode
```

#### Build JAVA ☕
```bash
ubuntu@ubuntu:~/code/bigdata-pipeline/etl$ mvn clean package
```

#### Run JAVA ☕
```bash
ubuntu@ubuntu:~/code/bigdata-pipeline/etl/target$ java -jar etl-x.x.x.jar
```