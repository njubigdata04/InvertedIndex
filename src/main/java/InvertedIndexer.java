import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexer {
    public static void main(String[] args){
        try{
            //Default Configuration
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "TF-IDF");

            job.setJarByClass((InvertedIndexer.class));

            job.setInputFormatClass(TextInputFormat.class);

            //set Mapper and Reducer
            job.setMapperClass(InvertedIndexMapper.class);
            job.setReducerClass(InvertedIndexReducer.class);

            //set Type of Outputs
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setCombinerClass(InvertedIndexCombiner.class);
            job.setPartitionerClass(InvertedIndexPartitioner.class);

            job.setNumReduceTasks(10);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            System.exit(job.waitForCompletion(true) ? 0:1);
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
