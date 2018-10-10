package com.raymon.hadoop.pagerank;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author zwg
 */
public class PageRankReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text arg0, Iterable<Text> arg1, Context arg2) throws IOException, InterruptedException {

        double sum = 0.0;
        Node sourceNode = null;

        for (Text i : arg1){
            Node node = Node.fromMR(i.toString());

           if (node.containsAdjacentNodes()){
               sourceNode = node;
           }else {
                sum = sum + node.getPageRank();
           }
        }

        double newPR = (1 - 0.85) / 4 + 0.85 * sum;
        System.out.println("*********** newPR is ************" + newPR);

        //把新的PR值和之前计算的PR值比较
        double dataPR = newPR - sourceNode.getPageRank();
        //先将差值放大1000倍
        int data = (int)(dataPR * 1000.0);

        //求绝对值
        data = Math.abs(data);
        System.out.println("************ data *************" + data);
        //获取计数器
        arg2.getCounter(RunJob.myCount.mycount).increment(data);
        sourceNode.setPageRank(newPR);
        arg2.write(arg0, new Text(sourceNode.toString()));
    }
}
