/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package pc1p7;

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
public class TMDBMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        if (key.get() == 0) {
            return;
        }
        String valueString = value.toString();
        String[] singleRowData = valueString.split(";");
        output.collect(new Text(singleRowData[10] + "," + singleRowData[1] + "," + singleRowData[8]), one);
    }
}

class TMDBMapper2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        String[] rowData = value.toString().split("\t");
        String[] rowValue = rowData[0].split(","); //companie, vote_average, popularity
        int N = Integer.parseInt(rowData[1]);
        output.collect(new Text(rowValue[0] + "," + rowValue[1]), new Text(rowValue[2] + "," + N));
    }
}

class TMDBMapper3 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        String[] rowData = value.toString().split("\t");
        String[] rowValue = rowData[0].split(",");
        double x = Double.parseDouble(rowValue[1]);
        double y = Double.parseDouble(rowData[1]);
        output.collect(new Text("b0,b1"), new Text(String.valueOf(x) + "," + String.valueOf(y)));
    }
}
