package test.indrajit.mapred;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * submit counter map-reduce job to hadoop cluster
 */
public class JobSubmitter extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JobSubmitter(), args);
        System.exit(res);
    }

    /**
     * @param args usage: [input] [output]
     * @return 0
     * @throws Exception
     */
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("usage: [input] [output]");
            System.exit(-1);
        }

        // create new job
        System.out.println("creating new job ....");
        Job job = Job.getInstance(new Configuration());
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // define mapper class and reducer class
        System.out.println("Mapper class : " + JobMapper.class.getName()
                + "  Reducer class : " + JobReducer.class.getName());
        job.setMapperClass(JobMapper.class);
        job.setReducerClass(JobReducer.class);

        // set input output formats
        System.out.println("Input format : " + TextInputFormat.class.getName()
                + " Output format : " + TextOutputFormat.class.getName());
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // set input output paths
        System.out.println("Input path : " + args[0] + " Output path : " + args[1]);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // set main job class
        System.out.println("Job class : " + JobSubmitter.class.getName());
        job.setJarByClass(JobSubmitter.class);
        job.setJobName("Itemref Counter_" + TimeStamp.getCurrentTime().toString());

        // submit job
        System.out.println("Submitting the job");
        job.submit();
        return 0;
    }
}