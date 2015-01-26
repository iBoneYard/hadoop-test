package test.indrajit.hadoop.anagrams;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Arrays;


public class WordMapper extends MapReduceBase implements
        Mapper<LongWritable, Text, Text, Text> {

    private Text sortedText = new Text();
    private Text orginalText = new Text();


    public void map(LongWritable key, Text value,
                    OutputCollector<Text, Text> outputCollector, Reporter reporter)
            throws IOException {

        String word = value.toString();
        char[] wordChars = word.toCharArray();
        Arrays.sort(wordChars);
        String sortedWord = new String(wordChars);
        sortedText.set(sortedWord);
        orginalText.set(word);
        outputCollector.collect(sortedText, orginalText);
    }

}
