package com.foo.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created with IntelliJ IDEA.
 * User: pascal
 * Date: 16-07-13
 * Time: 12:07
 */
public class WordMapper extends Mapper<LongWritable,Text,Text,Text> {

  private Text word = new Text();

  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
  {
    StringTokenizer itr = new StringTokenizer(value.toString(),",");
    while (itr.hasMoreTokens())
    {
      word.set(itr.nextToken());
      context.write(new Text(word.toString()), word);
    }
  }
}
