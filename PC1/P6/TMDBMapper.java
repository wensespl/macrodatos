/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package pc1p6;

import java.io.IOException;
import java.util.Random;
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
public class TMDBMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {    

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        if (key.get() == 0) {
            return;
        }
        String valueString = value.toString();
        String[] singleRowData = valueString.split(";");
        
        double runtime = Double.parseDouble(singleRowData[6]);
        double popularity = Double.parseDouble(singleRowData[8]);
        String status = singleRowData[3];
        
        output.collect(new Text(status), new Text(runtime + "," + popularity));
    }
}

class TMDBMapper2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {    

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] rowData = value.toString().split("\t");
        String[] centroidValues = rowData[0].split(";");
        String[] pointValues = rowData[1].split(",");
        
        double minDistance = Double.POSITIVE_INFINITY;
        double minxC = 0, minyC = 0;
        double xp = Double.parseDouble(pointValues[0]);
        double yp = Double.parseDouble(pointValues[1]);
        
        for (String centroidValue : centroidValues) {
            String[] centroidData = centroidValue.split(",");
            
            double cx = Double.parseDouble(centroidData[0]);
            double cy = Double.parseDouble(centroidData[1]);
            
            double dist = Math.sqrt(((cx-xp)*(cx-xp)) + ((cy-yp)*(cy-yp)));
            
            if (dist < minDistance) {
                minDistance = dist;
                minxC = cx;
                minyC = cy;
            }
        }
        
        output.collect(new Text(minxC + "," + minyC), new Text(rowData[1]));
    }
}