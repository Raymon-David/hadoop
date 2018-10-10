package com.raymon.hadoop.recommendfriends.util;

import org.apache.hadoop.io.Text;

//查找好友的好友
public class FridenOfFriend extends Text{

    public FridenOfFriend(){
        super();
    }

    public FridenOfFriend(String a , String b){
        super(getFriendOfFriend(a, b));
    }

    public static String getFriendOfFriend(String a , String b){
        int r = a.compareTo(b);
        if(r < 0){
            return a + "\t" + b;
        }else{
            return  b + "\t" + "a";
        }
    }
}
