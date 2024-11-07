/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package pc1p5a;

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
public class TMDBReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;
        double max = 0.0;
        String argmax = "";
            
        while (values.hasNext()) {
            Text value = (Text) values.next();
            String[] vals = value.toString().split(",");
            
            double popularity = Double.parseDouble(vals[0]);
            
            if (popularity > max) {
                max = popularity;
                argmax = vals[1];
            }
        }
        
        output.collect(key, new Text(argmax));
    }
}