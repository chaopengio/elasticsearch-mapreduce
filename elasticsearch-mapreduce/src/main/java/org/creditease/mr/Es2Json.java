package org.creditease.mr;

import java.io.IOException;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import org.elasticsearch.hadoop.mr.WritableArrayWritable;


public class Es2Json {
    public static class MapTask extends Mapper<Object, Object, Text, Text> {

        @Override
        protected void map(Object key, Object value, Context context)
               throws IOException, InterruptedException {
            Text docId = (Text) key;
            MapWritable valueMap = (MapWritable) value;
            
            JSONObject obj = es2Json(valueMap);
            context.write(docId, new Text(obj.toJSONString()));
        }
        
        public JSONObject es2Json(MapWritable valueWritable) throws IOException, InterruptedException {
            JSONObject obj = new JSONObject();
            for (Writable keyWritable : valueWritable.keySet()) {
                String key = ((Text) keyWritable).toString();
                Writable valWritable = valueWritable.get(keyWritable);
                
                if (valWritable instanceof Text) {
                    obj.put(key, valWritable.toString());
                } else if (valWritable instanceof IntWritable) {
                    obj.put(key, ((IntWritable)valWritable).get());
                } else if (valWritable instanceof FloatWritable) {
                    obj.put(key, ((FloatWritable)valWritable).get());
                } else if (valWritable instanceof LongWritable) {
                    obj.put(key, ((LongWritable)valWritable).get());
                } else if (valWritable instanceof DoubleWritable) {
                    obj.put(key, ((DoubleWritable)valWritable).get());
                } else if (valWritable instanceof BooleanWritable) {
                    obj.put(key, ((BooleanWritable)valWritable).get());
                } else if (valWritable instanceof MapWritable) {
                    obj.put(key, es2Json((MapWritable) valWritable));
                } else if (valWritable instanceof WritableArrayWritable) {
                    WritableArrayWritable waw = (WritableArrayWritable) valWritable;
                    Writable[] writable = waw.get();
                    
                    JSONArray array = new JSONArray();
                    for (int i=0; i < writable.length; ++i) {
                        Object o = writable[i];
                        if (o instanceof MapWritable) {
                            array.add(es2Json((MapWritable) o));
                        } else if (o instanceof Text) {
                            array.add(o.toString());
                        } else if (o instanceof IntWritable) {
                            array.add(((IntWritable)o).get());
                        } else if (o instanceof FloatWritable) {
                            array.add(((FloatWritable)o).get());
                        } else if (o instanceof LongWritable) {
                            array.add(((LongWritable)o).get());
                        } else if (o instanceof DoubleWritable) {
                            array.add(((DoubleWritable)o).get());
                        }
                    }
                    obj.put(key, array);
                }
            }
            return obj;
        }
    }
    
    public static class ReduceTask extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }
    
    public static void Run(String output, String outputFormat, int reducerNum, Configuration conf) 
            throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf);
//        job.setJobName(Es2Json.class.getName());
        job.setJarByClass(Es2Json.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        job.setMapperClass(MapTask.class);
        job.setReducerClass(ReduceTask.class);
        job.setInputFormatClass(EsInputFormat.class);
        
        if (outputFormat.equals("sequencefile")) {
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
        }
        
        job.setNumReduceTasks(reducerNum);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileOutputFormat.setOutputPath(job, new Path(output));
        
        job.setSpeculativeExecution(false);
        job.waitForCompletion(true);
    }

}
