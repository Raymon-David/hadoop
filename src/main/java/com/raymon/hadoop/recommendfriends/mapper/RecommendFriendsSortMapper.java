package com.raymon.hadoop.recommendfriends.mapper;

import com.raymon.hadoop.recommendfriends.util.User;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class RecommendFriendsSortMapper extends Mapper<Text, Text, User, User> {

    @Override
    protected void map(Text key, Text value,
                       Context context)
            throws IOException, InterruptedException {
        String[] args = StringUtils.split(value.toString(),'\t');
        String other = args[0];
        int friendsCount = Integer.parseInt(args[1]);

        context.write(new User(key.toString(),friendsCount), new User(other,friendsCount));
        context.write(new User(other,friendsCount), new User(key.toString(),friendsCount));
    }
}
