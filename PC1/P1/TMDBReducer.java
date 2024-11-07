/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package pc1p1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Collections;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author USUARIO
 */
public class TMDBReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text t_key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        Text key = t_key;
        double max = 0.0;
        double min = Double.POSITIVE_INFINITY;
        double sum = 0.0;
        
        List<Double> list = new ArrayList<Double>();
        
        int count = 0;
            
        while (values.hasNext()) {
            DoubleWritable value = (DoubleWritable) values.next();
            double vote_average = value.get();
            
            sum += vote_average;
            list.add(vote_average);
            
            if (vote_average > max) {
                max = vote_average;
            }
            
            if (vote_average < min) {
                min = vote_average;
            }
            
            count += 1;
        }
        
        Collections.sort(list);
        int length = list.size();
        double median;
        
        if (length == 2) {
            double medianSum = list.get(0) + list.get(1);
            median = medianSum / 2;
        } else if (length % 2 == 0) {
            double medianSum = list.get((length / 2) -1) + list.get(length / 2);
            median = medianSum / 2;
        } else {
            median = list.get(length / 2);
        }
        
        double mean = sum / count;
        double sumOfSquares = 0.0;
        
        for (double vote_average : list) {
            sumOfSquares += (vote_average - mean) * (vote_average - mean);
        }
        
        double std = Math.sqrt(sumOfSquares / count);
        
        String[] key_vals = key.toString().split(",");
        
        output.collect(new Text(key_vals[0]), new DoubleWritable(mean));
        output.collect(new Text(key_vals[1]), new DoubleWritable(median));
        output.collect(new Text(key_vals[2]), new DoubleWritable(std));
        output.collect(new Text(key_vals[3]), new DoubleWritable(max));
        output.collect(new Text(key_vals[4]), new DoubleWritable(min));
    }
}