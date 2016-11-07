package org.creditease.mr;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {

    public static void main(String [] args) throws Exception{
        int res = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(res);
    }
    
    public int run(String[] args) throws Exception {
        Options options = new Options();
        CommandLineParser parser = new BasicParser();
        
        options.addOption("job", true, "job");
        options.addOption("inputfile", true, "input path");
        options.addOption("outputfile", true, "output hdfs path");
        options.addOption("outputformat", true, "output hdfs format: text, sequencefile");
        options.addOption("reducernum", true, "reducer numbers");
        options.addOption("es_nodes", true, "es.nodes");
        options.addOption("es_resource", true, "es.resource");
        options.addOption("es_mapping_id", true, "es.mapping.id");
        options.addOption("es_mapping_setid", true, "es.mapping.setid: true, false");
        options.addOption("es_user", true, "es.net.http.auth.user");
        options.addOption("es_pswd", true, "es.net.http.auth.pass");

        options.addOption("map_mem_mb", true, "mapreduce.map.memory.mb:2048");
        options.addOption("map_java_opts", true, "mapreduce.map.java.opts:Xmx1500m");
        options.addOption("map_sort_mb", true, "mapreduce.task.io.sort.mb:256");
        
        CommandLine cmd = parser.parse(options, args);

        Configuration conf = this.getConf();
        
        conf.set("es.nodes", cmd.getOptionValue("es_nodes"));
        conf.set("es.resource", cmd.getOptionValue("es_resource"));
        
        conf.set("mapreduce.map.memory.mb", cmd.getOptionValue("map_mem_mb", "2048"));
        conf.set("mapreduce.map.java.opts", cmd.getOptionValue("map_java_opts", "-Xmx1500m"));
        conf.setInt("mapreduce.task.io.sort.mb", 256);

        String user = cmd.getOptionValue("es_user");
        String pswd = cmd.getOptionValue("es_pswd");
        if (user != null && pswd != null) {
            conf.set("es.net.http.auth.user", user);
            conf.set("es.net.http.auth.pass", pswd);
        }
        
        int reducerNum = Integer.parseInt(cmd.getOptionValue("reducernum"));
        
        if (reducerNum > 0) {
            conf.set("mapreduce.reduce.memory.mb", "2048");
            conf.set("mapred.reduce.child.java.opts", "-Xmx1500m");
            conf.setBoolean("mapred.compress.map.output", true);
            conf.setClass("mapred.map.output.compression.codec", GzipCodec.class, CompressionCodec.class);
            conf.setBoolean("mapred.output.compress", true);
            conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
        }
        
        String method = cmd.getOptionValue("job");
        
        if (method.equalsIgnoreCase("es2json")) {
            String outputFormat = cmd.getOptionValue("outputformat", "text");
            String output = cmd.getOptionValue("outputfile");
            Es2Json.Run(output, outputFormat, reducerNum, conf);
        } else if (method.equalsIgnoreCase("hdfs2es")) {
            conf.set("es.input.json", "yes");
            String input = cmd.getOptionValue("inputfile");
            String mappingId = cmd.getOptionValue("es_mapping_id");
            if (!mappingId.equalsIgnoreCase("null")) {
                conf.set("es.mapping.id", mappingId);
            }
            conf.set("es.mapping.setid", cmd.getOptionValue("es_mapping_setid", "false"));
            Hdfs2es.Run(input, conf);
        }
        
        return 0;
    }

}
