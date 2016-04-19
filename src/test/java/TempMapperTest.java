package java;

import com.siddhant.kaggle.maxtemperaturebystate.TempMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by siddhant on 4/19/16.
 */
public class TempMapperTest  {

    @Test
    public void mapperProcessValidRecord() throws IOException, InterruptedException {
        String val = "1856-10-01,25.195999999999998,1.048,Acre,Brazil";
        String[] valArr = val.split(",");
        String key = valArr[0];
        Text value = new Text(valArr[1] + "," + valArr[2] + "," + valArr[3] + "," + valArr[4]);
        new MapDriver<Text,Text,Text,Text>()
                .withMapper(new TempMapper())
                .withInput(new Text(key), value)
                .withOutput(new Text(key.split("-")[0]), new Text(value.toString().split(",")[0]))
                .runTest();



    }

    @Test
    public void mapperProcessEmptyKeyRecord() throws IOException, InterruptedException {
        String val = ",25.999,1.111,Acre,Brazil";
        String[] valArr = val.split(",");
        String key = valArr[0];
        Text value = new Text(valArr[1] + "," + valArr[2] + "," + valArr[3] + "," + valArr[4]);
        new MapDriver<Text,Text,Text,Text>()
                .withMapper(new TempMapper())
                .withInput(new Text(key), value)
                .runTest();
    }

    @Test
    public void mapperProcessEmptyTemperatureRecord() throws IOException, InterruptedException {
        String val = "1856-10-01,,1.048,Acre,Brazil";
        String[] valArr = val.split(",");
        String key = valArr[0];
        Text value = new Text(valArr[1] + "," + valArr[2] + "," + valArr[3] + "," + valArr[4]);
        new MapDriver<Text,Text,Text,Text>()
                .withMapper(new TempMapper())
                .withInput(new Text(key), value)
                .runTest();

    }
}
