package org.creditease.mr;

import java.io.IOException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

public class file2es extends Configured implements Tool {
    public static class MapTask extends Mapper<LongWritable, Text, NullWritable, Text> {
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String exists = context.getConfiguration().get("es.mapping.exists");
        }
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
               throws IOException, InterruptedException {
            String[] tks = StringUtils.split(value.toString(), "\t", 2);
            context.write(NullWritable.get(), new Text(tks[1]));
        }
    }
    
    
    
    public static class ReduceTask extends Reducer<Text, Text, NullWritable, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(NullWritable.get(), value);
            }
        }
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        CommandLineParser parser = new BasicParser();

        options.addOption("job", true, "job name");
        options.addOption("inputfile", true, "input path");
        options.addOption("es_nodes", true, "es.nodes");
        options.addOption("es_resource", true, "es.resource");
        options.addOption("es_mapping_id", true, "es.mapping.id");
        options.addOption("es_mapping_exists", true, "es.mapping.exists");
        
        CommandLine cmd = parser.parse(options, args);
        
        Configuration conf = this.getConf();
        
        if (cmd.hasOption("es_nodes")) {
            conf.set("es.nodes", cmd.getOptionValue("es_nodes"));
        } else {
            conf.set("es.nodes", "10.180.60.101,10.180.60.102,10.180.60.103");
        }
        
        conf.set("es.resource", cmd.getOptionValue("es_resource"));
        conf.set("es.input.json", "yes");

        if (cmd.hasOption("es_mapping_id")) {
            conf.set("es.mapping.id", cmd.getOptionValue("es_mapping_id"));
        }
        
        if (cmd.hasOption("es_mapping_exists")) {
            conf.set("es.mapping.exists", cmd.getOptionValue("es_mapping_exists"));
        }
        
        conf.set("mapreduce.map.memory.mb", "2048");
        conf.set("mapreduce.map.java.opts", "-Xmx1500m");
        conf.setInt("mapreduce.task.io.sort.mb", 256);
        
        Job job = Job.getInstance(conf);
        job.setJobName(cmd.getOptionValue("job"));
        
        job.setJarByClass(file2es.class);
        
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setMapperClass(MapTask.class);
        job.setOutputFormatClass(EsOutputFormat.class);
        
        job.setNumReduceTasks(0);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(cmd.getOptionValue("inputfile")));
        
//        job.setReducerClass(ReduceTask.class);
        
//        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        job.setSpeculativeExecution(false);
        job.waitForCompletion(true);
        return 0;
    }
    
    public static void main(String[] argv) throws Exception {
        int res = ToolRunner.run(new Configuration(), new file2es(), argv);
        System.exit(res);
	}
}
