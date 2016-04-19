package com.siddhant.kaggle.maxtemperaturebystate;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by siddhant on 4/18/16.
 */
public class TempMapper extends Mapper<Text, Text, Text, Text> {

    @Override
    public void map(Text key, Text value, Context context) {
        if (!key.toString().isEmpty()) {
            String state = context.getConfiguration().get(MapReduceDriver.STATE);
            String date = key.toString();
            String row = value.toString();
            String[] values = row.split(",");
            String year = date.split("-")[0];
            //System.out.println("MKey: " + key + " MValue: " + value);
            String header = "dt,AverageTemperature,AverageTemperatureUncertainty,State,Country";
            try {
                if (!header.equals(date + "," + row) && values.length == 4 && !values[0].isEmpty() &&
                        values[2].equalsIgnoreCase(state)) {
                    context.write(new Text(year), new Text(values[0]));
                }
            } catch (IOException e) {
                e.getCause();
            } catch (InterruptedException e) {
                e.getCause();
            }
        }



    }
}
