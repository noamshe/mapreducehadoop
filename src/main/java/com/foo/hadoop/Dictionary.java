package com.foo.hadoop;

    import org.apache.hadoop.conf.Configuration;
    import org.apache.hadoop.fs.Path;
    import org.apache.hadoop.io.Text;
    import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
    import org.apache.hadoop.mapreduce.Job;
    import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
    import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
    import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created with IntelliJ IDEA.
 * User: pascal
 * Date: 16-07-13
 * Time: 12:07
 */
public class Dictionary {

  public static void main(String[] args) throws Exception
  {
    /*
    HDFSClient client = new HDFSClient();
    client.readFile("/noam.txt");
    client.mkdir("/dearnoam");
    */
    Configuration conf = new Configuration();
    conf = new Configuration();
    conf.addResource(new Path("/data/hadoop-2.2.0/hadoop-2.2.0/etc/hadoop/core-site.xml"));
    conf.addResource(new Path("/data/hadoop-2.2.0/hadoop-2.2.0/etc/hadoop/hdfs-site.xml"));
    conf.addResource(new Path("/data/hadoop-2.2.0/hadoop-2.2.0/etc/hadoop/mapred-site.xml"));
    Job job = new Job(conf, "dictionary");
    job.setJarByClass(Dictionary.class);
    job.setMapperClass(WordMapper.class);
    job.setReducerClass(AllTranslationsReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    String hdfsInput = "/dearnoam";
    String hdfsOutput = "/thisistheoutput";
//    FileInputFormat.addInputPath(job, new Path(args[0]));
//    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    FileInputFormat.addInputPath(job, new Path(hdfsInput));
    FileOutputFormat.setOutputPath(job, new Path(hdfsOutput));
    boolean result = job.waitForCompletion(true);
    System.exit(result ? 0 : 1);
  }
}
