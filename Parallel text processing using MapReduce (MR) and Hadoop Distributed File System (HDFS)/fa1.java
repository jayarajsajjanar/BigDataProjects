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
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.ArrayList;


public class fa1 {
	static HashMap<String,ArrayList> hmap = new HashMap<String,ArrayList>();

	public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

		@Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	    	System.out.println("*******setup method called********");
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
	            		lem_list.add(words[i].toLowerCase());
	            	}
	            }
	            hmap.put(words[0].toLowerCase(),lem_list);
	            line=br.readLine();//second to last line reading
	        }
	        System.out.println("Size of hmap - "+hmap.size());
	        // Set keySet = new Set();
	        //Set keySet = hmap.keySet();
	        //for (Object key : keySet ){
	        //	System.out.println("key- "+key.toString()+" -value- "+hmap.get(key));
	        //}
	    }

       	public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
       		int begin = value.toString().indexOf("<");
       		int end = value.toString().indexOf(">");
   			StringBuilder tmp = new StringBuilder(); // Using default 16 character size
   			try{
	       		for (int i = begin;i<=end;i++){
	       			String temp = value.toString();
					tmp.append(temp.charAt(i));
	       		}

	       		String first_part = tmp.toString();
	       		String second_part = value.toString().substring(end+1,value.toString().length());

	       		//System.out.println("first_part - "+first_part+" -- second_part"+second_part);
		        
		        String[] result = second_part.toString().split("\\s");
		        Text first_part_text = new Text();
		        first_part_text.set(first_part);

	          	for (int i=0; i<result.length; i++){
	          		if(result[i].matches("[a-zA-Z]+")){
		  		        Text temp = new Text();
		  		        temp.set(result[i].toLowerCase());
		                //System.out.println("from mapper - "+temp.toString()+" - "+first_part.toString());
		  		        context.write(temp,first_part_text);
		  		    }
		        }
	    	}catch(Exception e){
	    		e.getStackTrace();
	    	}	
       	}
   	}

   	public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

			//Normalising the word - key 
			String norm_temp = key.toString().replace('j','i');
			String norm_temp2 = norm_temp.replace('v','u');


			//Extracting all the locations
			StringBuilder tmp_builder = new StringBuilder(); // Using default 16 character size

       		for (Text each_value: values){
       			String temp = each_value.toString();
				tmp_builder.append(temp);
				tmp_builder.append(" ");
       		}

       		String all_locs_string = tmp_builder.toString();
			//System.out.println("For normalised word: "+norm_temp2);

			if(hmap.containsKey(norm_temp2.toLowerCase())){
				ArrayList<String> lem_list = new ArrayList<String>();
				lem_list = hmap.get(norm_temp2);
				for (String lemmas: lem_list){
	                //System.out.println("from reducer - "+lemmas+" - "+all_locs_string);
	           		context.write(new Text(lemmas),new Text(all_locs_string));
				} 
			}else{
                //System.out.println("from reducer - "+norm_temp2+" - "+all_locs_string);
				context.write(new Text(norm_temp2),new Text(all_locs_string));
			}

    	}
    }

    public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(fa1.class);
    job.setMapperClass(TokenizerMapper.class);
    // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileInputFormat.addInputPath(job, new Path(args[1]));

    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}



