package test.indrajit.mapred;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JobMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable ONE = new IntWritable(1);
    private final static String PATTERN = "ref=['\"]plexina:/[a-z,A-z,0-9,/,-]+";

    @Override
    public void map(Object key, Text value, Context output) throws IOException,
            InterruptedException {

        Pattern pattern = Pattern.compile(JobMapper.PATTERN);
        Matcher matcher = pattern.matcher(value.toString());
        if (matcher.find()) {
            System.out.println(key.toString());
            String refString = matcher.group(0).replace("ref=\"", "").replace("ref='", "");
            String[] splitString = refString.split("/");
            Text refText = new Text(key.toString() + splitString[splitString.length - 1]);
            output.write(refText, JobMapper.ONE);
        }
    }
}