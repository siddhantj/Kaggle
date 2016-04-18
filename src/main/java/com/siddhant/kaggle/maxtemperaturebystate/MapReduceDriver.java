package com.siddhant.kaggle.maxtemperaturebystate;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Created by siddhant on 4/18/16.
 */
public class MapReduceDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MapReduceDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        try {
            getConf().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
            Job job = Job.getInstance(getConf(), "MapReduceDriver");
            job.setJarByClass(MapReduceDriver.class);
            job.setMapperClass(TempMapper.class);
            job.setReducerClass(TempReducer.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            int status = job.waitForCompletion(true) ? 0: 1;
            System.out.println(status);
            return status;

        } catch (IOException e) {
            e.getCause();
        } catch (InterruptedException e) {
            e.getCause();
        } catch (ClassNotFoundException e) {
            e.getCause();
        }

        return -1;
    }

}
