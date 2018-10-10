package com.raymon.hadoop.recommendfriends;

import com.raymon.hadoop.recommendfriends.mapper.RecommendFriendsMapper;
import com.raymon.hadoop.recommendfriends.mapper.RecommendFriendsSortMapper;
import com.raymon.hadoop.recommendfriends.reducer.RecommendFriendsReducer;
import com.raymon.hadoop.recommendfriends.reducer.RecommendFriendsSortReducer;
import com.raymon.hadoop.recommendfriends.util.FridenOfFriend;
import com.raymon.hadoop.recommendfriends.util.FriendsGroup;
import com.raymon.hadoop.recommendfriends.util.FriendsSort;
import com.raymon.hadoop.recommendfriends.util.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static java.lang.System.out;

public class RunJob {

    public static void main(String[] args) {
        //创建配置文件
        Configuration configuration = new Configuration();
        configuration.set("mapred.jar", "E:\\IDEA\\workspace\\mapreduce\\out\\artifacts\\mapreduce\\mapreduce.jar");

        if (run1(configuration)){
            run2(configuration);
        }
    }

    //对friend文件中的好友关系进行分析，并得出间接好友出现的次数
    public static boolean run1(Configuration configuration){
        try {
            //URI uri = new URI(hdfsUrl.trim());
            FileSystem fs = FileSystem.get(configuration);
            Job job = Job.getInstance(configuration);

            job.setJarByClass(com.raymon.hadoop.recommendfriends.RunJob.class);
            job.setJobName("recommendfriends");
            job.setMapperClass(RecommendFriendsMapper.class);
            job.setReducerClass(RecommendFriendsReducer.class);
            job.setMapOutputKeyClass(FridenOfFriend.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setInputFormatClass(KeyValueTextInputFormat.class);

            //input和output的路径是指在HDFS上的路径，不是操作系统上的路径
            FileInputFormat.addInputPath(job, new Path("/usr/hadoop/input/recommenfriends"));

            Path outpath = new Path("/usr/hadoop/output/recommenfriends");
            if(fs.exists(outpath)){
                fs.delete(outpath, true);
            }

            FileOutputFormat.setOutputPath(job, outpath);

            boolean f = job.waitForCompletion(true);
            if(f){
                out.println("job recommendfriends 任务执行成功");
            }
            return f;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    // 对间接关系的朋友排序
    public static boolean run2(Configuration configuration){
        try {
            //URI uri = new URI(hdfsUrl.trim());
            FileSystem fs = FileSystem.get(configuration);
            Job job = Job.getInstance(configuration);

            job.setJarByClass(com.raymon.hadoop.recommendfriends.RunJob.class);
            job.setJobName("friendssort");
            job.setMapperClass(RecommendFriendsSortMapper.class);
            job.setReducerClass(RecommendFriendsSortReducer.class);
            job.setSortComparatorClass(FriendsSort.class);
            job.setGroupingComparatorClass(FriendsGroup.class);
            job.setMapOutputKeyClass(User.class);
            job.setMapOutputValueClass(User.class);
            job.setInputFormatClass(KeyValueTextInputFormat.class);

            //input和output的路径是指在HDFS上的路径，不是操作系统上的路径
            FileInputFormat.addInputPath(job, new Path("/usr/hadoop/output/recommenfriends/part-r-00000"));

            Path outpath = new Path("/usr/hadoop/output/recommenfriends/friendssort");
            if(fs.exists(outpath)){
                fs.delete(outpath, true);
            }

            FileOutputFormat.setOutputPath(job, outpath);

            boolean f = job.waitForCompletion(true);
            if(f){
                out.println("job friendssort 任务执行成功");
            }
            return f;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

}
