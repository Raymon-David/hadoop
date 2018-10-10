package com.raymon.hadoop.recommendfriends.reducer;

import com.raymon.hadoop.recommendfriends.util.FridenOfFriend;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RecommendFriendsReducer extends Reducer<FridenOfFriend, IntWritable, FridenOfFriend, IntWritable>{

    @Override
     protected void reduce(FridenOfFriend arg10, Iterable<IntWritable> arg1, Context arg2) throws IOException, InterruptedException{
         int sum = 0;
         boolean f = true;
         for (IntWritable i : arg1){
             if (i.get() == 0){
                 f = false;
                 break;
             }else {
                 sum = sum + i.get();
             }
         }

         //不存在直接好友关系
         if (f){
             arg2.write(arg10, new IntWritable(sum));
         }
     }
}
