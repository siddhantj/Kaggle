package com.siddhant.kaggle.maxtemperaturebystate;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TempReducer extends Reducer<Text, Text, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) {
        String oldYear = null;
        double avgTemp = 0;
        int tempCount = 0;
        System.out.println("RKey: " + key);
        try {
            for (Text value : values ) {
                String year = key.toString();
                double temp = Double.parseDouble(value.toString());
                if (oldYear != null && !oldYear.equals(year) ) {
                    double avgTempYear = avgTemp/tempCount;
                    avgTemp = 0;
                    tempCount = 0;
                    context.write(key, new DoubleWritable(avgTempYear));
                }
                oldYear = year;
                avgTemp += temp;
                tempCount++;
            }

            context.write(key, new DoubleWritable(avgTemp/tempCount));  // last key
        } catch (IOException e) {
            e.getCause();
        } catch (InterruptedException e ){
            e.getCause();
        }

    }
}
