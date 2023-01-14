package com.shiyanlou.simulator;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class FileWrite {

    // The configuration information class of the job,
    // any role configuration information must be passed through Configuration
    // because the Configuration enables information sharing between multiple
    // mapper and multiple reducer tasks
    private static Configuration configuration;

    static {
        try {
            //initialize configuration
            configuration = new Configuration();
            configuration.set("fs.default.name", "hdfs://localhost:9000");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static List<String> readFile(String path) {
        //1.The list object accesses the contents of the read file
        List<String> list = new ArrayList<String>();
        try {
            //2.File Read Stream
            FileReader fr = new FileReader(new File(path));
            //Buffering with BufferedReader
            BufferedReader br = new BufferedReader(fr);
            //3.Open up string storage space for receiving read file data
            String line;
            //4.Read until there are no more lines to read and then end
            while ((line = br.readLine()) != null) {
                //write into ArrayList
                list.add(line);
            }
            //4.close resource
            fr.close();
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        //return list
        return list;
    }

    /*
     * method description: write file to hdfs
     * parameter description: hdfsPath (hdfs file system needs to receive data file path, upload.txt path, here I enter: hdfs:/upload.txt)
     * list readFile() method return value, that is, the need to write the data
     */
    public static void writeFile(String hdfsPath, List<String> list) {
        try {

            //1,Get hdfs file path
            FileSystem fileSystem = FileSystem.get(configuration);
            //2.hdfs file output stream
            FSDataOutputStream fsDataOutputStream = fileSystem.append(new Path(hdfsPath));
            //3.Iterate through a list collection and write to a file
            for (String s : list) {
                fsDataOutputStream.writeBytes(s);
            }
            //4.Close resource
            fileSystem.close();
            fsDataOutputStream.close();
            System.out.println("successfully import");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        List<String> strings = FileWrite.readFile("./record.txt");
        FileWrite.writeFile("hdfs:/upload.txt",strings);
    }}






