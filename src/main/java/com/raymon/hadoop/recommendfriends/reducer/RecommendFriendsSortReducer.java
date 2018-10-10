package com.raymon.hadoop.recommendfriends.reducer;

import com.raymon.hadoop.recommendfriends.util.User;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RecommendFriendsSortReducer extends Reducer<User, User, Text, Text> {

    @Override
    protected void reduce(User arg0, Iterable<User> arg1,
                          Context arg2)
            throws IOException, InterruptedException {
        String user = arg0.getUname();
        StringBuffer sb = new StringBuffer();
        for(User u: arg1 ){
            sb.append(u.getUname()+":"+ u.getFriendsCount());
            sb.append(",");
        }
        arg2.write(new Text(user), new Text(sb.toString()));
    }
}
