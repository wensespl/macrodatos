/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package pc1test;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author USUARIO
 */
public class PC1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        if (key.get() == 0) {
            return;
        }
        String valueString = value.toString();
        String[] singleRowData = valueString.split(",");
        
        String country = singleRowData[7];
        if(!"Country".equals(country)){
            int price = Integer.parseInt(singleRowData[2]);
        
            output.collect(new Text(country), new IntWritable(price));
        }
    }
}

class PC1Mapper2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        if (key.get() == 0) {
            return;
        }
        String[] rowData = value.toString().split("\t");
        
        String country = rowData[0];
        int ventas_total = Integer.parseInt(rowData[1]);
        
        output.collect(new Text("Media global,Mediana global"), new Text(ventas_total + "," + country));
    }
}
