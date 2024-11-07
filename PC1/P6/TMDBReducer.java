/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package pc1p6;

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
        String status = key.toString();

        double xc1 = Math.random() * 14400;
        double xc2 = Math.random() * 14400;
        double xc3 = Math.random() * 14400;
        double xc4 = Math.random() * 14400;
        double xc5 = Math.random() * 14400;
        double xc6 = Math.random() * 14400;

        double yc1 = Math.random() * 5741.978;
        double yc2 = Math.random() * 5741.978;
        double yc3 = Math.random() * 5741.978;
        double yc4 = Math.random() * 5741.978;
        double yc5 = Math.random() * 5741.978;
        double yc6 = Math.random() * 5741.978;

        while (values.hasNext()) {
            Text value = (Text) values.next();
            String[] vals = value.toString().split(",");

            output.collect(new Text(xc1 + "," + yc1 + ";" + xc2 + "," + yc2 + ";" + xc3 + "," + yc3 + ";" + xc4 + "," + yc4 + ";" + xc5 + "," + yc5 + ";" + xc6 + "," + yc6), new Text(vals[0] + "," + vals[1] + "," + status));
        }
    }
}

class TMDBReducer2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;
        String[] centroidValues = key.toString().split(",");
        
        double sumxC = 0, sumyC = 0;
        String cLabel = "";
        int count = 0;

        while (values.hasNext()) {
            Text value = (Text) values.next();
            String[] vals = value.toString().split(",");
            
            sumxC += Double.parseDouble(vals[0]);
            sumyC += Double.parseDouble(vals[1]);
            cLabel = vals[2];
            
            count += 1;
        }
        
        double xC = sumxC / count;
        double yC = sumyC / count;
        
        output.collect(new Text(cLabel), new Text(xC + "," + yC));
    }
}