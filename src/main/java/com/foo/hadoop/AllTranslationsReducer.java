package com.foo.hadoop;

    import org.apache.hadoop.io.Text;
    import org.apache.hadoop.mapreduce.Reducer;

    import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: pascal
 * Date: 17-07-13
 * Time: 19:50
 */
public class AllTranslationsReducer extends Reducer<Text, Text, Text, Text> {

  private Text result = new Text();

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    int sum = 0;

    for (Text val : values) {
      sum++;
    }

    result.set(String.valueOf(sum));
    context.write(key, result);
  }
}
