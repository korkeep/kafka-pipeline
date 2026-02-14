package com.processor;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import com.util.PropertyFileReader;

public class Hdfs2Kafka {
    
    private Properties systemProp = null;
    private FileSystem hadoopFs = null;

    public Hdfs2Kafka() throws Exception{
        systemProp = PropertyFileReader.readPropertyFile("SystemConfig.properties");
        String HADOOP_CONF_DIR = systemProp.getProperty("hadoop.conf.dir");

        Configuration conf = new Configuration();
        conf.addResource(new Path("file:///" + HADOOP_CONF_DIR + "/core-site.xml"));
        conf.addResource(new Path("file:///" + HADOOP_CONF_DIR + "/hdfs-site.xml"));

        String namenode = systemProp.getProperty("hdfs.namenode.url");
        hadoopFs = FileSystem.get(new URI(namenode), conf);
    }

    public List<String> readHadoopFile(String filename) throws Exception {
        Path path = new Path(filename);
        List<String> lines = new ArrayList<>();

        if(!hadoopFs.exists(path)){
            System.out.println("readHadoopFile: file does not exist");
            return null;
        }

        FSDataInputStream input = hadoopFs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(input));

        while(br.ready()){
            String[] line = br.readLine().split(",");
            lines.add(line[2] + "," + line[3] + "," + line[4] + "," + line[5] + "," +
                        line[6] + "," + line[7] + "," + line[8] + "," + line[9]);
        }

        br.close();
        input.close();
        return lines;
    }

    public void getHadoopFilesInfo(String filename) throws Exception {
        Path path = new Path(filename);
        FileStatus fStatus = hadoopFs.getFileStatus(path);
        
        if(fStatus.isFile()){
            System.out.println("getHadoopFilesInfo: block size = " + fStatus.getBlockSize());
            System.out.println("getHadoopFilesInfo: group = " + fStatus.getGroup());
            System.out.println("getHadoopFilesInfo: owner = " + fStatus.getOwner());
            System.out.println("getHadoopFilesInfo: length = " + fStatus.getLen());
        }
        else{
            System.out.println("getHadoopFilesInfo: file does not exist");
        }
    }

    public void sendLines2Kafka(String topic, String line){
        System.out.println(line);
    }

    public void closeStream() throws Exception{
        if(hadoopFs != null){
            hadoopFs.close();
        }
    }

}
