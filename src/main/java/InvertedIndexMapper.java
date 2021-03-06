import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Longwritable key: LineNo; Text value: text of the while line;

        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName();

        Text word = new Text();
        StringTokenizer itr = new StringTokenizer(value.toString());
        for(; itr.hasMoreTokens();){

            //word#filename
            word.set(itr.nextToken() + "#" + fileName);
            context.write(word, new IntWritable(1));
        }
    }

}