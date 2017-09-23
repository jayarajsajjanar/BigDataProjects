import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCo3 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private IntWritable pos = new IntWritable(1);
    private IntWritable one = new IntWritable(1);

    private Text pair = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      String[] result = value.toString().split("\\s");
      System.out.println("--"+value.toString());
      //String[] result = "one two three four".split("\\s");
      for (int i=0; i<result.length; i++){
        System.out.println(result[i]);

        if(result[i].matches("[a-zA-Z0-9]+")){
        	// int x = 0;
        	// int y = 0;
        	for (int k=i-1;k>=0;k--){
            if(result[k].matches("[a-zA-Z0-9]+")){
          	  // pos.set(x++);
          	  pair.set(result[i]+" "+result[k]);
          	  context.write(pair, one);
             }
        	}
        	for (int l=i+1;l<result.length;l++){
            if(result[l].matches("[a-zA-Z0-9]+")){
          	  // pos.set(++y);
          	  pair.set(result[i]+" "+result[l]);
            	  context.write(pair, one);
            }
        	}			
        }
      }
          //while (itr.hasMoreTokens()) {
          //  word.set(itr.nextToken());
          //  context.write(word, one);
          //}
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
        //int val2 = val.get();
        //result.set(val2);
        //context.write(key,result );
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCo3.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
