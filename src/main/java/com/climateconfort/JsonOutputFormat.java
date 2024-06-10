package com.climateconfort;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JsonOutputFormat extends FileOutputFormat<Text, FloatWritable> {

    @Override
    public RecordWriter<Text, FloatWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Path outputPath = getDefaultWorkFile(job, ".json");
        FileSystem fs = outputPath.getFileSystem(job.getConfiguration());
        FSDataOutputStream fileOut = fs.create(outputPath, false);
        return new JsonRecordWriter(fileOut);
    }
}
