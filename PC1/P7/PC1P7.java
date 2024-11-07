/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Main.java to edit this template
 */
package pc1p7;

import org.apache.hadoop.fs.Path;
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
public class PC1P7 {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            JobClient my_client = new JobClient();

            // Configuración del primer job
            JobConf job_conf1 = new JobConf(PC1P7.class);
            job_conf1.setJobName("PC1P7 1");
            job_conf1.setOutputKeyClass(Text.class);
            job_conf1.setOutputValueClass(IntWritable.class);
            job_conf1.setMapperClass(TMDBMapper.class);
            job_conf1.setReducerClass(TMDBReducer.class);
            job_conf1.setInputFormat(TextInputFormat.class);
            job_conf1.setOutputFormat(TextOutputFormat.class);
            FileInputFormat.setInputPaths(job_conf1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job_conf1, new Path(args[1]));
            my_client.setConf(job_conf1);
            JobClient.runJob(job_conf1);

            // Configuración del segundo job
            JobConf job_conf2 = new JobConf(PC1P7.class);
            job_conf2.setJobName("PC1P7 2");
            job_conf2.setOutputKeyClass(Text.class);
            job_conf2.setOutputValueClass(Text.class);
            job_conf2.setMapperClass(TMDBMapper2.class);
            job_conf2.setReducerClass(TMDBReducer2.class);
            job_conf2.setInputFormat(TextInputFormat.class);
            job_conf2.setOutputFormat(TextOutputFormat.class);
            FileInputFormat.setInputPaths(job_conf2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job_conf2, new Path(args[2]));
            my_client.setConf(job_conf2);
            JobClient.runJob(job_conf2);

            // Configuración del tercer job
            JobConf job_conf3 = new JobConf(PC1P7.class);
            job_conf3.setJobName("PC1P7 3");
            job_conf3.setOutputKeyClass(Text.class);
            job_conf3.setOutputValueClass(Text.class);
            job_conf3.setMapperClass(TMDBMapper3.class);
            job_conf3.setReducerClass(TMDBReducer3.class);
            job_conf3.setInputFormat(TextInputFormat.class);
            job_conf3.setOutputFormat(TextOutputFormat.class);
            FileInputFormat.setInputPaths(job_conf3, new Path(args[1])); // Cambiar a args[2] si es necesario
            FileOutputFormat.setOutputPath(job_conf3, new Path(args[3])); // Cambiar a args[3] si es necesario
            my_client.setConf(job_conf3);
            JobClient.runJob(job_conf3);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
