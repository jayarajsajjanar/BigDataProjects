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
import org.apache.hadoop.io.MapWritable;
import java.util.*; 
import org.apache.hadoop.io.Writable;


public class WordCoStripes3 {

  static class MyMapWritable extends MapWritable {
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        Set<Writable> keySet = this.keySet();

        for (Object key : keySet) {
            // System.out.println("key - "+key);
            // System.out.println("value - "+this.get(key) );
            result.append("<" + key.toString() + " , " + this.get(key) + ">");
        }
        return result.toString();
    }
  }


  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, MyMapWritable>{

    private IntWritable pos = new IntWritable(1);
    private IntWritable one = new IntWritable(1);
    private MyMapWritable occurrenceMap = new MyMapWritable();

    private Text neighbor = new Text();
    private Text cur_token = new Text();


    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      String[] result = value.toString().split("\\s");
      System.out.println("--"+value.toString());
      //String[] result = "one two three four".split("\\s");
      for (int i=0; i<result.length; i++){
        System.out.println(result[i]);
        cur_token.set(result[i]);
        occurrenceMap.clear();

        if(result[i].matches("[a-zA-Z0-9]+")){
        	// int x = 0;
        	// int y = 0;
        	for (int k=i-1;k>=0;k--){
            if(result[k].matches("[a-zA-Z0-9]+")){
          	  // pos.set(x++);
          	  //pair.set(result[i]+" "+result[k]);
          	  //context.write(pair, one);
              neighbor.set(result[k]);
              System.out.println("Adding - "+neighbor.toString()+"- result value - "+result[k] + " - k - "+k);
              Text neig_text = new Text();
              neig_text.set(neighbor.toString());
              // occurrenceMap.put(neighbor,new IntWritable(1));
              if(!occurrenceMap.containsKey(neig_text)){
                System.out.println("Does not exist");
                occurrenceMap.put(neig_text,new IntWritable(1));
                System.out.println("Status - "+cur_token+" - "+occurrenceMap.toString());
              }else{
                System.out.println("Exists");
                IntWritable temp = (IntWritable)occurrenceMap.get(neig_text);
                occurrenceMap.put(neig_text,new IntWritable(temp.get()+1));
                System.out.println("Status - "+cur_token+" - "+occurrenceMap.toString());
              }

             }
        	}
        	for (int l=i+1;l<result.length;l++){
            if(result[l].matches("[a-zA-Z0-9]+")){
          	  // pos.set(++y);
          	  //pair.set(result[i]+" "+result[l]);
          	  //context.write(pair, one);
              neighbor.set(result[l]);
              System.out.println("Adding - "+neighbor.toString()+"- result value - "+result[l] + " - l - "+l);
              Text neig_text = new Text();
              neig_text.set(neighbor.toString());
              // occurrenceMap.put(neighbor,new IntWritable(1));
              //occurrenceMap.put(neig_text,new IntWritable(1));
              
              if(!occurrenceMap.containsKey(neig_text)){
                System.out.println("Does not exist");
                occurrenceMap.put(neig_text,new IntWritable(1));
                System.out.println("Status - "+cur_token+" - "+occurrenceMap.toString());
              }else{
                System.out.println("Exists");
                IntWritable temp = (IntWritable)occurrenceMap.get(neig_text);
                occurrenceMap.put(neig_text,new IntWritable(temp.get()+1));
                System.out.println("Status - "+cur_token+" - "+occurrenceMap.toString());
              }

              System.out.println("Status - "+cur_token+" - "+occurrenceMap.toString());

            }
        	}

          Set<Writable> keySet = occurrenceMap.keySet();

          for (Object keyy : keySet) {
              System.out.print("Displaying locally key - "+keyy);
              System.out.print(" - Displaying locally value - "+occurrenceMap.get(keyy)+"\n" );
          }

          System.out.println("from mapper - "+cur_token+" - "+occurrenceMap.toString());
          context.write(cur_token,occurrenceMap);			
        }
      }
          //while (itr.hasMoreTokens()) {
          //  word.set(itr.nextToken());
          //  context.write(word, one);
          //}
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,MyMapWritable,Text,MyMapWritable> {
    private IntWritable result = new IntWritable();
    private MyMapWritable incrementingMap = new MyMapWritable();  

    public void reduce(Text key, Iterable<MyMapWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      // int sum = 0;
      // for (IntWritable val : values) {
      //   sum += val.get();
      //   //int val2 = val.get();
      //   //result.set(val2);
      //   //context.write(key,result );
      // }
      // result.set(sum);
      // context.write(key, result);
//******************************
      incrementingMap.clear();
      for (MyMapWritable each_value : values) {

        Set<Writable> keys = each_value.keySet();
        for (Writable each_key : keys) {
            IntWritable fromCount = (IntWritable) each_value.get(each_key);
            if (incrementingMap.containsKey(each_key)) {
                IntWritable count = (IntWritable) incrementingMap.get(each_key);
                count.set(count.get() + fromCount.get());
            } else {
                incrementingMap.put(each_key, fromCount);
            }
        }

      }
      System.out.println("From reducer - "+key + " - "+ incrementingMap.toString());
      context.write(key, incrementingMap);
//******************************
      // incrementingMap.clear();
      // for (MyMapWritable each_value : values) {

      //   Set<Text> keys = each_value.keySet();
      //   for (Text each_key : keys) {
      //       IntWritable fromCount = (IntWritable) each_value.get(each_key);
      //       if (incrementingMap.containsKey(each_key)) {
      //           IntWritable count = (IntWritable) incrementingMap.get(each_key);
      //           count.set(count.get() + fromCount.get());
      //       } else {
      //           incrementingMap.put(each_key, fromCount);
      //       }
      //   }

      // }
      // context.write(key, incrementingMap);

    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCoStripes3.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MyMapWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
