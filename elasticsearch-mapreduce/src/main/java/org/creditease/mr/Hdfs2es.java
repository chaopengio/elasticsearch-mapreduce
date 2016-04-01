package org.creditease.mr;

import java.io.IOException;

import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

public class Hdfs2es {
    public static class MapTask extends Mapper<Text, Text, NullWritable, Text> {
        private boolean needSetId = false;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String setId = context.getConfiguration().get("es.mapping.setid");
            if (setId.equals("false")) {
                needSetId = false;
            } else {
                needSetId = true;
            }
        }
        
        @Override
        protected void map(Text key, Text value, Context context)
               throws IOException, InterruptedException {
            if (!needSetId) {
                context.write(NullWritable.get(), value);
            } else {
                JSONObject obj = (JSONObject) JSONValue.parse(value.toString());
                obj.put("id", key.toString());
                context.write(NullWritable.get(), new Text(obj.toJSONString()));
            }
        }
    }
    
    public static void Run(String input, Configuration conf) 
            throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf);
        job.setJobName(Hdfs2es.class.getName());
        job.setJarByClass(Hdfs2es.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setMapperClass(MapTask.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(EsOutputFormat.class);
        
        job.setNumReduceTasks(0);
        
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
        
        
        job.setSpeculativeExecution(false);
        job.waitForCompletion(true);
    }
}
