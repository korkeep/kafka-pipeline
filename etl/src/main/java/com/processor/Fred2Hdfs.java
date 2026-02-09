package com.processor;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.pojo.EtlColumnPojo;
import com.pojo.FredColumnPojo;
import com.util.PropertyFileReader;
import com.util.US_STATES;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

public class Fred2Hdfs {
    
    public enum APITYPE{
        SEARCH("series/search"),
        OBSERVATION("series/observations");

        private String apiType;

        APITYPE(String apiType){
            this.apiType = apiType;
        }
    }

    public enum FREQUENCY{
        MONTH('M'),
        YEAR('A');

        private char freq;

        FREQUENCY(char freq){
            this.freq = freq;
        }
    }
    
    private Properties fredProp = null;
    private ObjectMapper mapper = null;
    private FileSystem hadoopFs = null;
    
    public Fred2Hdfs() throws Exception{
        fredProp = PropertyFileReader.readPropertyFile("SystemConfig.properties");
        String HADOOP_CONF_DIR = fredProp.getProperty("hadoop.conf.dir");

        mapper = new ObjectMapper();

        Configuration conf = new Configuration();
        conf.addResource(new Path("file:///" + HADOOP_CONF_DIR + "/core-site.xml"));
        conf.addResource(new Path("file:///" + HADOOP_CONF_DIR + "/hdfs-site.xml"));

        String namenode = fredProp.getProperty("hdfs.namenode.url");
        hadoopFs = FileSystem.get(new URI(namenode), conf);
    }

    public List<EtlColumnPojo> getEtlListData(FREQUENCY freq, US_STATES state, String searchText) throws Exception {
        String fredUrl = fredProp.getProperty("fred.url");
        String apiKey = fredProp.getProperty("fred.apiKey");
        String fileType = "json";
        String searchUrl = fredUrl + APITYPE.valueOf("SEARCH").apiType + "?search_text=" + searchText.replace(' ', '+') + 
                            state.getName().replaceAll(" ", "+") + "&api_key=" + apiKey + "&file_type=" + fileType;
        
        System.out.println(searchUrl);
    
        JsonNode rootNode = mapper.readTree(new URL(searchUrl));
        Thread.sleep(500);
        ArrayNode nodeSeries = (ArrayNode)rootNode.get("series");
        List<FredColumnPojo> listFredData = mapper.readValue(nodeSeries.traverse(), new TypeReference<List<FredColumnPojo>>(){});
        Predicate<FredColumnPojo> predi = 
            fred -> (
                fred.getTitle().equals(searchText + state.getName()) &&
                (fred.getFrequency_short().charAt(0) == freq.freq) &&
                fred.getSeasonal_adjustment().equals("NSA")
            );
        List<EtlColumnPojo> listData = listFredData.stream().filter(predi).flatMap(pojo -> {
            String observeUrl = fredUrl + APITYPE.valueOf("OBSERVATION").apiType +
                                "?series_id=" + pojo.getId() + "&api_key=" + apiKey + "&file_type=" + fileType;
    
            System.out.println(observeUrl);
    
            try{
                JsonNode nodeValue = mapper.readTree(new URL(observeUrl));
                Thread.sleep(500);
                ArrayNode nodeValueObserve = (ArrayNode)nodeValue.get("observations");
                List<EtlColumnPojo> listEtlData = mapper.readValue(nodeValueObserve.traverse(), new TypeReference<List<EtlColumnPojo>>(){});
    
                for(EtlColumnPojo valuePojo : listEtlData){
                    valuePojo.setState(state.getName());
                    valuePojo.setId(pojo.getId());
                    valuePojo.setTitle(pojo.getTitle().replace(',','_'));
                    valuePojo.setFrequency_short(pojo.getFrequency_short());
                    valuePojo.setUnits_short(pojo.getUnits_short());
                    valuePojo.setSeasonal_adjustment_short(pojo.getSeasonal_adjustment_short());           
                }
                return listEtlData.stream();
            }
            catch(Exception e) {
                e.printStackTrace();
            }
            return Stream.empty();
            
        }).collect(Collectors.toList());  
        
        return listData;
    }

    public void writeCsv2Hdfs(String filename, List<EtlColumnPojo> nodeList) throws Exception{
        Path hadoopPath = new Path(filename);

        FSDataOutputStream hadoop0utStream = null;
        BufferedWriter bw = null;

        if(nodeList.size() != 0){
            if(hadoopFs.exists(hadoopPath)){
                hadoop0utStream = hadoopFs.append(hadoopPath);
                bw = new BufferedWriter(new OutputStreamWriter(hadoop0utStream, StandardCharsets.UTF_8));
            }
            else{
                hadoop0utStream = hadoopFs.create(hadoopPath, true);
                bw = new BufferedWriter(new OutputStreamWriter(hadoop0utStream, StandardCharsets.UTF_8));
                bw.write(nodeList.get(0).getColumns());
                bw.newLine();
            }

            for (EtlColumnPojo pojo : nodeList){
                bw.write(pojo.getValues());
                bw.newLine();
            }
    
            bw.close();
            hadoop0utStream.close();
        }
    }


    public void writeCsv2Local(boolean first, String path, String filename, List<EtlColumnPojo> nodeList) throws Exception {
        CsvMapper csvMapper = new CsvMapper();
        csvMapper.configure(JsonGenerator.Feature.IGNORE_UNKNOWN, true);
        csvMapper.findAndRegisterModules();
        csvMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        CsvSchema schema = csvMapper.schemaFor(EtlColumnPojo.class).withColumnSeparator(',');
        if(first){
            schema = schema.withHeader();
        }
        else{
            schema = schema.withoutHeader();
        }

        File outputFile = new File(path + filename);
        OutputStream os = new FileOutputStream(outputFile, true);
        ObjectWriter ow = csvMapper.writer(schema);
        ow.writeValue(os, nodeList);
        os.close();
    }

    public void clearInputFiles(String path, String filename) throws Exception{
        Path hadoopPath = new Path(filename);

        if(hadoopFs.exists(hadoopPath)){
            hadoopFs.delete(hadoopPath, true);
            System.out.println("clearInputFiles: " + hadoopPath.getName() + "file deleted");
        }

        File localPath = new File(path + filename);
        if(localPath.exists()){
            localPath.delete();
            System.out.println("clearInputFiles: " + localPath.getName() + "file deleted");
        }
    }

    public void closeStream() throws Exception{
        if(hadoopFs != null){
            hadoopFs.close();
        }
    }
}
