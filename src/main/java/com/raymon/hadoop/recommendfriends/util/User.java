package com.raymon.hadoop.recommendfriends.util;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class User implements WritableComparable<User> {
    private String uname;

    public String getUname() {
        return uname;
    }

    public void setUname(String uname) {
        this.uname = uname;
    }

    public int getFriendsCount() {
        return friendsCount;
    }

    public void setFriendsCount(int friendsCount) {
        this.friendsCount = friendsCount;
    }

    private int friendsCount;

    public User() {
        // TODO Auto-generated constructor stub
    }

    public User(String uname,int friendsCount){
        this.uname=uname;
        this.friendsCount=friendsCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(uname);
        out.writeInt(friendsCount);
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        this.uname=in.readUTF();
        this.friendsCount=in.readInt();
    }

    @Override
    public int compareTo(User o) {
        int result = this.uname.compareTo(o.getUname());
        if(result==0){
            return Integer.compare(this.friendsCount, o.getFriendsCount());
        }
        return result;
    }
}
