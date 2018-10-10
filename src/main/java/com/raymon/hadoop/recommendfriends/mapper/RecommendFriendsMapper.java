package com.raymon.hadoop.recommendfriends.mapper;

import com.raymon.hadoop.recommendfriends.util.FridenOfFriend;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class RecommendFriendsMapper extends Mapper<Text, Text, FridenOfFriend, IntWritable>{

    /**
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException{
        String user = key.toString();
        String[] friends = StringUtils.split(value.toString(), '\t');

        for (int i = 0; i < friends.length; i++){
            String f1 = friends[i];
            //假的 好友的好友 关系， 即直接好友关系
            FridenOfFriend ofof = new FridenOfFriend(user, f1);
            context.write(ofof, new IntWritable(0));

            for (int j = i + 1; j < friends.length; j++){
                String f2 = friends[j];
                FridenOfFriend fof = new FridenOfFriend(f1, f2);
                context.write(fof, new IntWritable(1));
            }
        }
    }
}
