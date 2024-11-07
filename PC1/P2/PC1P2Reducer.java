/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package pc1p2;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author USUARIO
 */
public class PC1P2Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;
        double max = 0.0;
        String argmax = "";
        while (values.hasNext()) {
            // replace type of value with the actual type of our value
            Text value = (Text) values.next();
            String[] vals = value.toString().split(";");
            System.out.println(vals[1]);
            double populatity = Double.parseDouble(vals[1]);
            if (populatity > max) {
                max = populatity;
                argmax = vals[0];
            }
        }
        output.collect(key, new Text(argmax));
    }
}
