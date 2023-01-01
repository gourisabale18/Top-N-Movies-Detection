package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class MovieReducer extends Reducer<Text, LongWritable,LongWritable,Text> {

    TreeMap<Long,String> tmap;

    @Override
    public void setup(Context context)
    {
        tmap=new TreeMap<Long, String>();
    }

    @Override
    public void reduce(Text key, Iterable<LongWritable> values,
                       Context context) {
        String name = key.toString();
        long count = 0;

        for (LongWritable val : values) {
            count = val.get();
        }
        tmap.put(count, name);
        if (tmap.size() > 10)
        {
            tmap.remove(tmap.firstKey());
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for(Map.Entry<Long,String> entry: tmap.entrySet())
        {
            long count= entry.getKey();
            String key=entry.getValue();
            context.write(new LongWritable(count),new Text(key));
        }
    }

}
