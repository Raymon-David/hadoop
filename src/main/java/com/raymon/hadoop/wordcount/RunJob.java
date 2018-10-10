package com.raymon.hadoop.wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static java.lang.System.*;

/**
 * @author zwg
 */
public class RunJob {
    //public static String hdfsUrl = "hdfs://10.40.59.154:9000";

    public static void main(String[] args) {
        //创建配置文件
        Configuration configuration = new Configuration();
        configuration.set("mapred.jar", "/Users/raymon/Documents/software/workspace/hadoop/target/hadoop-mapreduce-1.0-SNAPSHOT.jar");

        try {
            //URI uri = new URI(hdfsUrl.trim());
            FileSystem fs = FileSystem.get(configuration);
            Job job = Job.getInstance(configuration);

            job.setJarByClass(RunJob.class);
            job.setJobName("wc");
            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            //input和output的路径是指在HDFS上的路径，不是操作系统上的路径
            FileInputFormat.addInputPath(job, new Path("/usr/hadoop/input/wc.txt"));

            Path outpath = new Path("/usr/hadoop/output");
            if(fs.exists(outpath)){
                fs.delete(outpath, true);
            }

            FileOutputFormat.setOutputPath(job, outpath);

            boolean f = job.waitForCompletion(true);
            if(f){
                out.println("job任务执行成功");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
