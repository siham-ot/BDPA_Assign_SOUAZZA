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

/*Prefix filtering principale 
-Index only the first |d| - ⌈t •|d|⌉ + 1 words of each document d, 
without missing any similar documents 
(t is the Jaccard similarity threshold and |d| is the number of words in d)*/

public class SetSimilarityB {


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "SetSimilarityB");
    job.setJarByClass(SetSimilarityB.class);
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
    public static Float Threshold = 0.8f;


//the mapper creates inverted index for the first words of each document
    
  public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text index = new Text();
	
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] doc = value.toString().split("\t")[1].split(" ");
		//|d| - ⌈t •|d|⌉ + 1 
		int prefixindex = ((Double) (doc.length - Math.ceil(Threshold * doc.length) + 1)).intValue();//index of words
			for (int i = 0; i < prefixindex; i += 1) {
				index.set(doc[i]);
				context.write(index, value);//out is the key of the words and the documents 
			}
    }
  }

 //The reducer takes both key and value and computes the similarity of the documents pairs
 //It output only the similar pairs as well as the value of their jaccard similarity
  public static class JoinReducer extends Reducer<Text,Text,Text,FloatWritable> {
	  	static enum CountersEnum {COMPARISONS}
		private Text index = new Text();
		private FloatWritable output = new FloatWritable();
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<Long> id = new ArrayList<Long>();
		List<String> lines = new ArrayList<String>();
		//the reducer will iterate through the entire document of the pre-processed input file
			for (Text out : values) {
				id.add(Long.parseLong(out.toString().split("\t")[0]));
				lines.add(out.toString().split("\t")[1]);
			}
			//It performs pair wise similarities 
			for (int i = 0; i < id.size(); i += 1) {
				Long id1 = id.get(i);
				String line1 = lines.get(i);
				    for (int j = i + 1; j < id.size(); j += 1) {
				    	Long id2 = id.get(j);
				    	String line2 = lines.get(j);
				    	Float similar = Similarity(line1, line2);
				    	Counter iter = context.getCounter(CountersEnum.class.getName(),CountersEnum.COMPARISONS.toString());
						iter.increment(1);
				    		if (similar> Threshold) { //comparing similairty to threshold
				    			String idset = (id1 < id2) ? Long.toString(id1) + "|" + Long.toString(id2) : Long.toString(id2) + "|" + Long.toString(id1);
				    			index.set(idset);
				    			output.set(similar);
				    			context.write(index, output);
				    		}
				    }
			}
    }
		
  public static Float Similarity(String doc1, String doc2) {
		// similarity(q,r) = ∣q ∩ r∣ / min{∣q∣, ∣r∣} 				
		Set<String> doc1Set = new HashSet<String>(Arrays.asList(doc1.split(" ")));
		Set<String> doc2Set = new HashSet<String>(Arrays.asList(doc2.split(" ")));
		Set<String> intersection = new HashSet<String>(doc2Set);
		Set<String> union = new HashSet<String>(doc1Set);
		union.addAll(doc2Set);
		intersection.retainAll(doc1Set);
			
		return (new Float(intersection.size())) / (new Float(union.size()));
		}
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

