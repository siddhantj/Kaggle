import com.siddhant.kaggle.maxtemperaturebystate.TempReducer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by siddhant on 4/19/16.
 */
public class TempReducerTest  {

    @Test
    public void computeAverageOnReducer() throws IOException, InterruptedException {
        new ReduceDriver<Text,Text,Text,DoubleWritable>()
                .withReducer(new TempReducer())
                .withInput(new Text("1950"), Arrays.asList(new Text("13"), new Text("14")))
                .withOutput(new Text("1950"), new DoubleWritable(13.5))
                .runTest();
    }
}
