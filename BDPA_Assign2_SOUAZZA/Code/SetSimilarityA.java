package Assignment2;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*Pre-proccessing file displays all lines with unique words and no stop words.
The first approach to finding similar sets is the naive approach that performs all pairwise comparisons between documents and then output only the pairs that are similar
The assignment is giving us a threshold of 0.8. 
Due to the excessive runtimes, we only input the first 100 lines of the output pre-processed as a sample. 
then will increase the number to have a decently sized output for similar sets. 
*/
public class SetSimilarityA {
	
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "SetSimilarityA");
		    job.setJarByClass(SetSimilarityA.class);
		    job.setMapperClass(JoinMapper.class);
		    job.setReducerClass(JoinReducer.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(FloatWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }

  public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text>{
  		
		private Text outkey = new Text();
		private Text line = new Text();
		
		/*Defining the number of lines to read, we start initially with a sample 		
		 * of 100. The long is to define the id number of each line keeping in mind 		
		 * that each line is considered as a document. The int can be changed depending on the file size*/
		public Long ID = 1000L;//100 is PreProcessing_Input100
		
		// the mapper generates a value and key for each document id.
		public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
			/*Defining the number of each line aka document
			the key generated from the mapper is the number of both documents 
			we extract their line text that will later be processed by the reducer to be able to compute similarity */
			Long documentID = Long.parseLong(value.toString().split("\t")[0]);
			line.set(value.toString().split("\t")[1]);
			//http://event.cwi.nl/SIGMOD-RWE/2010/17-8cbba9/paper.pdf

			//the loop iterates through each line/document of the pre processed file and write a key value
			
			for (Long id = 1L; id < documentID; id = id + 1L) {
				outkey.set(Long.toString(id)+"/"+Long.toString(documentID));
				context.write(outkey, line);
			}
		

			for (Long id = documentID + 1L; id < ID + 1L; id = id + 1L) {
				outkey.set(Long.toString(documentID)+"/"+ Long.toString(id));
				context.write(outkey, line);
			}
    }
  }


/*In the reducer, the values of each document is read and interate through it 
It then compares the values and computes the similarity
The similarity is compared to the threshold.  */
public static class JoinReducer extends Reducer<Text,Text,Text,FloatWritable> {

		static enum CountersEnum {COMPARISONS}
		public static Float Threshold = 0.8f;
		private FloatWritable output = new FloatWritable();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      
				String line1 = values.iterator().next().toString();
				String line2 = values.iterator().next().toString();	
				
				Counter iter = context.getCounter(CountersEnum.class.getName(),CountersEnum.COMPARISONS.toString());
				iter.increment(1);
			
				Float similarity = Similarity(line1, line2); //Similarity is a function defined to compute the jaccard similarity
					if (similarity > Threshold) {
						output.set(similarity);
						context.write(key, output);
					}
		}	
//To compute the similarity, this class used the jaccard similarity equation
		

		public static Float Similarity(String line1, String line2) {
			//transfroms documents into harshest 
			Set<String> hashline1 = new HashSet<String>(Arrays.asList(line1.split(" ")));
			Set<String> hashline2 = new HashSet<String>(Arrays.asList(line2.split(" ")));
			
			//Find the union and intersection between both documents 
			Set<String> union = new HashSet<String>(hashline1);
			Set<String> intersection = new HashSet<String>(hashline2);
			
			union.addAll(hashline2);
			intersection.retainAll(hashline1);
			//using jaccard similarity equation: intersection/union 
			return (new Float(intersection.size())) / (new Float(union.size()));
	
	/*https://dukesoftware00.blogspot.fr/2014/11/java-compute-source-code-similarity.html
	public class JaccardSimlarity<T> {
			private final Set<T> intersect = new HashSet<>();
			private final Set<T> union = new HashSet<>();
			public double compute(Set<T> docset1, Set<T> docset2) {
				intersect.clear();
			 	intersect.addAll(docset1);
				intersect.retainAll(docset2);
				union.clear();
				union.addAll(docset1);
				union.addAll(docset2);
			return (new Float(intersection.size())) / (new Float(union.size()));
			 }
	}*/
	}
  }

}



