package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;

public class MovieMapper extends Mapper<LongWritable, Text,Text, LongWritable> {

    TreeMap<Long,String> treeMap;
    @Override
    public void setup(Context context) throws IOException,
            InterruptedException
    {
        treeMap = new TreeMap<Long, String>();
    }

    @Override
    public void map(LongWritable key, Text value, Context context
    ) throws IOException, InterruptedException {

        //split the line using /t
        //Movie Details in the form of Movie Name and No of Views
        String[] movieDetails= value.toString().split("\t");
        String movieName=movieDetails[0];
        Long noOfViews=Long.parseLong(movieDetails[1]);
        //In tree map, movies would be placed in sorted order
        treeMap.put(noOfViews,movieName);
        //since we want top 10 movies ,if there are more movies in a list then we can remove
        //start key and value pair from tree map because they will least no od views
        //At the end ,we will get only top no of views movies only
        if(treeMap.size()>10)
        {
            treeMap.remove(treeMap.firstKey());
        }

    }

    //cleanup() method which runs only once at the end in the lifetime of Mapper.
    //Mapper processes one key-value pair at a time and writes them as intermediate output on local
    // disk. But we have to process whole block (all key-value pairs) to find top10, before
    // writing the output, hence we use context.write() in cleanup().

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Map.Entry<Long, String> entry : treeMap.entrySet())
        {
            long count=entry.getKey();
            String value=entry.getValue();
            context.write(new Text(value),new LongWritable(count));
        }

    }
}
