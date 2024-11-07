/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package pc1test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
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
public class PC1Reducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {

    public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;
        
        int sum = 0;
            
        while (values.hasNext()) {
            IntWritable value = (IntWritable) values.next();
            int price = value.get();
            
            sum += price;
        }
        
        output.collect(new Text(key), new Text(String.valueOf(sum)));
    }
}

class PC1Reducer2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;
        double sum = 0.0;
        
        List<Integer> list = new ArrayList<Integer>();
        List<String> list_values = new ArrayList<String>();
        
        int count = 0;
            
        while (values.hasNext()) {
            Text value = (Text) values.next();
            String[] data_values = value.toString().split(",");
            
            int ventas_total = Integer.parseInt(data_values[0]);
            
            sum += ventas_total;
            list.add(ventas_total);
            list_values.add(value.toString());
            
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
        
        double dif = Double.POSITIVE_INFINITY;
        String pais_prox = "";
        
        for (String list_value : list_values) {
            String[] venta_pais = list_value.split(",");
            int venta_total_pais = Integer.parseInt(venta_pais[0]);
            String pais = venta_pais[1];
            
            double dif_media = Math.abs(venta_total_pais - mean);
            
            if (dif_media < dif) {
                dif = dif_media;
                pais_prox = pais;
            }
        }
        
        String[] key_vals = key.toString().split(",");
        
        output.collect(new Text(key_vals[0]), new Text(String.valueOf(mean)));
        output.collect(new Text(key_vals[1]), new Text(String.valueOf(median)));
        output.collect(new Text("Pais prox"), new Text(pais_prox));
    }
}