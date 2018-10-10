package com.raymon.hadoop.recommendfriends.util;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FriendsGroup extends WritableComparator {

    public FriendsGroup(){
        super(User.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        User u1 = (User) a;
        User u2 = (User) b;

        return u1.getUname().compareTo(u2.getUname());
    }
}
