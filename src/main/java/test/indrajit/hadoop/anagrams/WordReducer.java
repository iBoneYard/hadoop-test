package test.indrajit.hadoop.anagrams;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class WordReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();


    public void reduce(Text anagramKey, Iterator<Text> anagramValues,
                       OutputCollector<Text, Text> results, Reporter reporter) throws IOException {
        String output = "";
        while (anagramValues.hasNext()) {
            Text anagam = anagramValues.next();
            output = output + anagam.toString() + "~";
        }
        StringTokenizer outputTokenizer = new StringTokenizer(output, "~");
        if (outputTokenizer.countTokens() >= 2) {
            output = output.replace("~", ",");
            outputKey.set(anagramKey.toString());
            outputValue.set(output);
            results.collect(outputKey, outputValue);
        }
    }

}
