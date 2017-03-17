package Assignment2;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class PreProcessing {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Pre Processing");
        
        job.setJarByClass(PreProcess.class); 
        job.setMapperClass(JoinMapper.class); 
        job.setNumReduceTasks(1); 
        job.setReducerClass(JoinReducer.class); 
        
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
        
    try {
            Long RecordNum = job.getCounters().findCounter("org.apache.hadoop.mapred.Task.Counter", "Record").getValue();
            BufferedWriter writer = new BufferedWriter(new FileWriter("/home/cloudera/Desktop/Record.txt"));
            writer.write(String.valueOf(RecordNum));
            writer.close();
        } catch(Exception e) {
            throw new Exception(e);
        }
        
          System.exit(0);
    }
 
    public static class JoinMapper extends Mapper<Text, Text, Text, Text> {
              private Text word = new Text();
              
         //Upload The stop words.txt file from Assignment 1    
             File StopWords = new File("/home/cloudera/Desktop/stopwords.txt");
         //Save the txt file into a Hashset
             Set<String> StopWordsSet = new HashSet<String>();//HashSets take unique words, there is no repetition
             
                protected void ReadStopWord(Context context) throws IOException, InterruptedException { //Reading stop words  
                  BufferedReader readSW = new BufferedReader(new FileReader(StopWords)); 
                  String stopword = null; //opening the file to read stop words + initialisation to null
                  while ((stopword = readSW.readLine()) != null){ 
                      StopWordsSet.add(stopword);
                  }//Putting all stopwords within a set
                  readSW.close();
             }
              @Override
              public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
                 
                 List <String> Count = new ArrayList <>();
                 if (value.toString().length() != 0){ //find empty lines and deletes them
                    for (String str : value.toString().replaceAll("[^A-Za-z0-9]"," ").split("\\s+")){ //only leaves values with A-Z, a-z, 0-9 and replace words
                        if (!StopWordsSet.contains(str.toLowerCase())){ //If the word is not a stop word
                            if (!Count.contains(str.toLowerCase())){ //If it is not reported in the document 
                                Count.add(str.replaceAll("[^A-Za-z0-9]","").toString().toLowerCase()); 
                                word.set(str.toString());
                                if(word.toString().length() != 0){
                                    context.write(key, word);// Mapper will find all words that are A-Z, a-z, 0-9, non stop words and no empty lines
                                }
                            }
                        }
                    }
                 }
              }
           }

    public static class JoinReducer extends Reducer<Text, Text, LongWritable, Text> {
        private HashMap<String, Integer> Docfreq = new HashMap<String, Integer>(); //Hash map will be each word and its frequency stored 
        private long iter = 0; //keep track of number of documents printed (only works if using one reducer)
        
        //las palabras se ordenan antes de los id de documento
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int frequency = 0; //Frequency of tokens 
            LinkedList<String> WordDoc = new LinkedList<String>(); //Each document’s word is stored 
            for(Text valeur : values) {
                if(key.charAt(0) != '{') { //https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String/charAt
                    frequency += Integer.parseInt(valeur.toString());
                } 
                else { //word counts ids 
                    String[] MotValeur = valeur.toString().split("\\s");
                    for(int i = 0; i < MotValeur.length; i++) {
                        int k = 0;
                        String mot = MotValeur[i];//
                        if(!WordDoc.contains(mot)) { //
                            int addfrequency = Docfreq.get(mot);
                            while(k < WordDoc.size() && addfrequency > Docfreq.get(WordDoc.get(k))) { //get position in set 
                                k++;
                            }
                            WordDoc.add(k, mot); //add the word to the list
                        }
                    }
                }
            }
            if(key.charAt(0) != '{') { //put both word and frequency 
                Docfreq.put(key.toString(), frequency);
            }
            else {
                String index = key.toString(); //key to string
                int index1 = index.lastIndexOf("#") + 1;
                long id = Long.parseLong(index.substring(index1));
                String output = "";
                for(String s : WordDoc) {
                    output = output + s + " ";
                }
                if(iter == 0) { //
                    iter = id + 1;
                }
                context.write(new LongWritable(iter), new Text(output)); //write in output the counter for word frequency and the word 
                iter++; //increment and loop back 
            }
        }
    }//Reducer takes word and frequency and prepares the output preprocessed documents as asked for the requirements. 
}//El Fin