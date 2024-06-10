package com.climateconfort;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AvroDecodeDriver {
    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(AvroDecodeDriver.class);
        job.setJobName("Decode Avro Files");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
            
        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(JsonOutputFormat.class);

        Schema schema = new Schema.Parser().parse(
            AvroDecodeDriver.class.getResourceAsStream("/sensor_data.avsc"));
        AvroJob.setInputKeySchema(job, schema);

        job.setMapperClass(AvroDecodeMapper.class);
        job.setReducerClass(AvroDecodeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
