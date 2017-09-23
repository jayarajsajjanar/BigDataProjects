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
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.PrintWriter;


import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.ArrayList;



public class fa2a_script {

  static HashMap<String,ArrayList> hmap = new HashMap<String,ArrayList>();


  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private IntWritable pos = new IntWritable(1);
    private IntWritable one = new IntWritable(1);

    private Text pair = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      System.out.println("*******setup method called********");
      // Path pt=new Path("hdfs://localhost:9000/home/hadoop/input/new_lemmatizer.csv");
      Path pt=new Path("hdfs://localhost:9000/home/hadoop/input/new_lemmatizer.csv");

        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        line=br.readLine();
          // HashMap<String,ArrayList> hmap = new HashMap<String,ArrayList>();

        while (line != null){
            //System.out.println(line);
            String[] words = line.split(",");
            ArrayList<String> lem_list = new ArrayList<String>();
            for (int i=1; i<words.length;i++){
              if(words[i]!=null){
                lem_list.add(words[i]);
              }
            }
            hmap.put(words[0],lem_list);
            line=br.readLine();//second to last line reading
        }
        System.out.println("Size of hmap - "+hmap.size());
        // Set keySet = new Set();
        //Set keySet = hmap.keySet();
        //for (Object key : keySet ){
        //  System.out.println("key- "+key.toString()+" -value- "+hmap.get(key));
        //}
    }

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      //StringTokenizer itr = new StringTokenizer(value.toString());
      //String[] result = value.toString().split("\\s");
      // System.out.println("--"+value.toString());

      String line = value.toString();

      int begin = line.indexOf("<");
      int end = line.indexOf(">");

      String first_part = line.substring(begin,end);

      String second_part = line.substring(end+1,line.length());

      String[] result = second_part.toLowerCase().split("\\s");


      //String[] result = "one two three four".split("\\s");
      for (int i=0; i<result.length; i++){
        // System.out.println(result[i]);

        if(result[i].matches("[a-zA-Z0-9]+")){
        	// int x = 0;
        	// int y = 0;
        	for (int k=i-1;k>=0;k--){
            if(result[k].matches("[a-zA-Z0-9]+")){
          	  // pos.set(x++);
          	  pair.set(result[i]+" "+result[k]);
          	  context.write(pair, new Text(first_part));
             }
        	}
        	for (int l=i+1;l<result.length;l++){
            if(result[l].matches("[a-zA-Z0-9]+")){
          	  // pos.set(++y);
          	  pair.set(result[i]+" "+result[l]);
          	  context.write(pair, new Text(first_part));
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
  public static class CoCombiner
         extends Reducer<Text,Text,Text,Text> {
      private IntWritable result = new IntWritable();

      public void reduce(Text key, Iterable<Text> values,
                         Context context
                         ) throws IOException, InterruptedException {
        String  all_locs = "";
        // System.out.println("Null found : "+ (values.size()<1?"true":"false"));
        for (Text val : values) {
            all_locs = all_locs + val.toString()+" ";
        }
        
        context.write(key,new Text(all_locs));
    }
  }



  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      String norm_temp = key.toString().replace('j','i');
      String norm_temp2 = norm_temp.replace('v','u');

      String[] key_arr = norm_temp2.split(" ");

      String first = key_arr[0].toLowerCase();
      String second = key_arr[1].toLowerCase();


      String  all_locs = "";
      for (Text val : values) {
          all_locs = all_locs + val.toString()+" ";
      }


      if(hmap.containsKey(first)){

            ArrayList<String> first_lem_list = new ArrayList<String>();
            first_lem_list = hmap.get(first);

            for (String first_lemmas: first_lem_list){

                if(hmap.containsKey(second)){
                  
                      ArrayList<String> second_lem_list = new ArrayList<String>();
                      second_lem_list = hmap.get(second);
                      
                      for (String second_lemmas: second_lem_list){
                                //System.out.println("from reducer - "+lemmas+" - "+all_locs_string);
                              context.write(new Text(first_lemmas+" "+second_lemmas),new Text(all_locs));
                      } 
                }else{
                          //System.out.println("from reducer - "+norm_temp2+" - "+all_locs_string);
                      context.write(new Text(first_lemmas+" "+second),new Text(all_locs));
                }
            }
      }else{

            if(hmap.containsKey(second)){
                
                    ArrayList<String> second_lem_list = new ArrayList<String>();
                    second_lem_list = hmap.get(second);
                    
                    for (String second_lemmas: second_lem_list){
                              //System.out.println("from reducer - "+lemmas+" - "+all_locs_string);
                            context.write(new Text(first+" "+second_lemmas),new Text(all_locs));
                    } 
            }else{
                        //System.out.println("from reducer - "+norm_temp2+" - "+all_locs_string);
                    context.write(new Text(first+" "+second),new Text(all_locs));
            }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(fa2a_script.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(CoCombiner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));


    long startTime = System.currentTimeMillis();
    boolean status = job.waitForCompletion(true);
    long total = System.currentTimeMillis() - startTime;
    System.out.println("Total run time: "+total);

    FileWriter fw = new FileWriter("j.txt", true);
    BufferedWriter bw = new BufferedWriter(fw);
    PrintWriter out = new PrintWriter(bw);
    out.println(""+total);
    out.close();

    System.exit(status ? 0 : 1);  
}
}
