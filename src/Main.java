import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;


public class Main  {
    public static void main(String[] argv) throws Exception
    {
        Configuration conf=new Configuration();
        Job job=new Job(conf,"Inverted Index");

        job.setJarByClass(Main.class);

        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(Combiner.class);

        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if(argv.length!=3)
        {
            System.err.println("Usage: InvertedIndex <in> <out>");
            System.exit(2);
        }

        FileInputFormat.addInputPath(job,new Path(argv[1]));
        FileOutputFormat.setOutputPath(job,new Path(argv[2]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
