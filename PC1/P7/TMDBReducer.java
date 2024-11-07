/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package pc1p7;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author USUARIO
 */
public class TMDBReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        Text key = t_key;
        int frequencyForCompanie = 0, cont = 0;

        while (values.hasNext()) {
            // replace type of value with the actual type of our value
            cont = values.next().get();
            frequencyForCompanie += cont;
        }
        output.collect(key, new IntWritable(frequencyForCompanie));
    }
}

class TMDBReducer2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key; // companie, vote_average
        int num_records = 0;
        double max = 0;
        double popularity = 0;
        int N = 0;
        while (values.hasNext()) { //popularity, N
            // replace type of value with the actual type of our value
            Text value = (Text) values.next();
            String[] vals = value.toString().split(",");
            popularity = Double.parseDouble(vals[0]);
            if (popularity > max) {
                max = popularity;
                N = Integer.parseInt(vals[1]);
            }
        }

        output.collect(key, new Text(String.valueOf(max/N)));
    }
}

class TMDBReducer3 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;
        double Sx = 0.0, Sy = 0.0, Sxy = 0.0, Sx2 = 0.0;
        int n = 0;
        double x, y, b0, b1;

        while (values.hasNext()) {
            // replace type of value with the actual type of our value
            Text value = (Text) values.next();
            String[] vals = value.toString().split(","); //[x,y]
            x = Double.parseDouble(vals[0]);
            y = Double.parseDouble(vals[1]);
            Sx += x;
            Sy += y;
            Sxy += x * y;
            Sx2 += x * x;
            n++;
        }
        b0 = (Sy*Sx2 - Sx*Sxy)/(n * Sx2 - Sx * Sx);
        b1 = (n*Sxy - Sx*Sy)/(n * Sx2 - Sx * Sx);
        String[] txt_key = key.toString().split(",");
        output.collect(new Text(txt_key[0]), new Text(String.valueOf(b0)));
        output.collect(new Text(txt_key[1]), new Text(String.valueOf(b1)));
    }
}