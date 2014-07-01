package com.ccri;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount extends Configured
        implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        ToolRunner.run(new WordCount(), new String[] {});

//        ToolRunner.run(conf, new RemoteTool, Array(
//                "-libjars", tempFile.getAbsolutePath,
//                classOf[RemoteTool].getCanonicalName,
//                "--geomesa.temp.jar", tempFile.getAbsolutePath))
    }

    @Override
    public int run(String[] args)
            throws Exception {
        Configuration conf = getConf();

        conf.set("mapreduce.map.class", "com.ccri.WordCountMap");
        conf.set("mapreduce.combine.class", "com.ccri.WordCountReduce");
        conf.set("mapreduce.reduce.class", "com.ccri.WordCountReduce");

        FileSystem fs = FileSystem.get(conf);

        Path outputDir = new Path("/user/emilio/output/");
        if (fs.exists(outputDir)) {
            // remove this directory, if it already exists
            fs.delete(outputDir, true);
        }

        Job job = Job.getInstance(conf, "emilio-wordcount");

        job.setJarByClass(WordCount.class);
//    println(tempFile.getAbsolutePath)

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileOutputFormat.setOutputPath(job, outputDir);

        FileInputFormat.setInputPaths(job, new Path("/user/emilio/input/"));

        //      job.setJarByClass(SensorIngest.class)

        boolean result = job.waitForCompletion(true);

        System.out.println("done with job: " + result);

        if (result) return 0; else return 1;

    }
}
