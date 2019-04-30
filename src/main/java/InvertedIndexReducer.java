import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {
    private List<String> TermList = new ArrayList<String>();
    private Map<String, Integer> WordFrequencyMap = new HashMap<String, Integer>();
    private Map<String, Integer> WordExistDocFrequencyMap = new HashMap<String, Integer>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //key: word#filename

        Iterator<IntWritable> it = values.iterator();

        String word = key.toString().split("#")[0];
        String fileName = key.toString().split("#")[1];
        String authorName = NovelNameProcess.splitAuthorName(fileName);

        if(word.equals("")){
            word = "emptystr";
        }
        if(authorName.equals("")){
            authorName = "emptyauthor";
        }

        String term = word + "#" + authorName;

        if(!TermList.contains(term))
            TermList.add(term);

        int sum = 0;
        for(IntWritable val : values){
            sum += val.get();
        }

        if(WordFrequencyMap.containsKey(term)){
            int wordFrequency = WordFrequencyMap.get(term);

            wordFrequency += sum;

            WordFrequencyMap.put(term, wordFrequency);
        }
        else{
            WordFrequencyMap.put(term, sum);
        }

        if(WordExistDocFrequencyMap.containsKey(term)){
            int frequency = WordExistDocFrequencyMap.get(term);

            frequency += 1;

            WordExistDocFrequencyMap.put(term, frequency);
        }
        else{
            WordExistDocFrequencyMap.put(term, 1);
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(String term : TermList){
            String authorName = term.split("#")[1];
            String word = term.split("#")[0];
            if(!authorName.equals("emptyauthor") && !word.equals("emptystr")) {
                String out = word + "," + WordFrequencyMap.get(term).toString() + "-" +
                        FilesInfo.calcIDF(WordExistDocFrequencyMap.get(term));
                context.write(new Text(authorName), new Text(out));
            }
        }
    }

}
