package com.climateconfort;

import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvroDecodeMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, Text, FloatWritable> {

    private String[] keys = {"temperature", "soundLevel", "humidity", "pressure"};

    @Override
    protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
        GenericRecord record = key.datum();

        // Extract values and write to context
        for (String loopKey : keys) {
            Float fieldValue = (Float) record.get(loopKey);
            if (fieldValue != null) {
                context.write(new Text(loopKey), new FloatWritable(fieldValue));
            }
        }
    }
}
