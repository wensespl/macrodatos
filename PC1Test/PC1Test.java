/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package pc1test;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 *
 * @author USUARIO
 */
public class PC1Test {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        JobClient my_client = new JobClient();
        JobConf job_conf1 = new JobConf(PC1Test.class);
        job_conf1.setJobName("PC1Test");
        job_conf1.setOutputKeyClass(Text.class);
        job_conf1.setOutputValueClass(IntWritable.class);
        job_conf1.setMapperClass(pc1test.PC1Mapper.class);
        job_conf1.setReducerClass(pc1test.PC1Reducer.class);
        job_conf1.setInputFormat(TextInputFormat.class);
        job_conf1.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job_conf1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_conf1, new Path(args[1]));
        my_client.setConf(job_conf1);
        
        
        JobClient my_client2 = new JobClient();
        JobConf job_conf2 = new JobConf(PC1Test.class);
        job_conf1.setJobName("PC1Test2");
        job_conf1.setOutputKeyClass(Text.class);
        job_conf1.setOutputValueClass(Text.class);
        job_conf1.setMapperClass(pc1test.PC1Mapper2.class);
        job_conf1.setReducerClass(pc1test.PC1Reducer2.class);
        job_conf1.setInputFormat(TextInputFormat.class);
        job_conf1.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job_conf2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job_conf2, new Path(args[2]));
        

        try {
            JobClient.runJob(job_conf1);
            JobClient.runJob(job_conf2);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
