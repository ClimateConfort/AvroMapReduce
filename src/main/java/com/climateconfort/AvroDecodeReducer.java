package com.climateconfort;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvroDecodeReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{

    @Override
    protected void reduce(Text key, Iterable<FloatWritable> values,
            Reducer<Text, FloatWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
        
        float sum = 0;
        int count = 0;
        for (FloatWritable loopValue : values){
            sum += loopValue.get();
            count += 1;
        } 

        float mean = sum / count; 
        String newKey = key.toString()+"Mean";

        context.write(new Text(newKey), new FloatWritable(mean));
    }
}
