package com.climateconfort;

import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import com.google.gson.stream.JsonWriter;

public class JsonRecordWriter extends RecordWriter<Text, FloatWritable> {

    private final JsonWriter jsonWriter;

    public JsonRecordWriter(DataOutputStream out) {
        this.jsonWriter = new JsonWriter(new java.io.OutputStreamWriter(out));
        try {
            this.jsonWriter.beginArray(); // Begin the JSON array
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, FloatWritable value) throws IOException {
        jsonWriter.beginObject();
        jsonWriter.name(key.toString()).value(value.get());
        jsonWriter.endObject();
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException {
        try {
            jsonWriter.endArray(); // End the JSON array
            jsonWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
