package com.raymon.hadoop.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static java.lang.System.out;

public class RunJob {

    public static enum myCount{
        mycount;
    };

    public static void main(String[] args){

        //创建配置文件
        Configuration configuration = new Configuration();
        configuration.set("mapred.jar", "E:\\IDEA\\workspace\\mapreduce\\out\\artifacts\\mapreduce\\mapreduce.jar");

        int i = 0;
        double pr = 0.001;
        while (true){
            i++;

            try {
                configuration.setInt("runCount", 1);

                FileSystem fs = FileSystem.get(configuration);
                Job job = Job.getInstance(configuration);

                job.setJarByClass(com.raymon.hadoop.pagerank.RunJob.class);
                job.setJobName("pagerank" + i);
                job.setMapperClass(PageRankMapper.class);
                job.setReducerClass(PageRankReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                job.setInputFormatClass(KeyValueTextInputFormat.class);

                //input和output的路径是指在HDFS上的路径，不是操作系统上的路径
                Path inputPath = new Path("/usr/hadoop/input/pagerank");

                if (i > 1){
                    inputPath = new Path("/usr/hadoop/output/pagerank" + (i - 1));
                }
                FileInputFormat.addInputPath(job, inputPath);

                Path outpath = new Path("/usr/hadoop/output/pagerank" + i);
                if(fs.exists(outpath)){
                    fs.delete(outpath, true);
                }

                FileOutputFormat.setOutputPath(job, outpath);

                boolean f = job.waitForCompletion(true);
                if(f){
                    out.println("job PageRank " + i + " 任务执行成功");

                    //获取所有节点的差值， 利用MapReduce的计数器
                    long sum = job.getCounters().findCounter(myCount.mycount).getValue();
                    System.out.println("************* sum ***********" + sum);
                    //放大10000倍
                    double avg = sum / 4000.0;

                    if (avg < pr){
                        break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

}
