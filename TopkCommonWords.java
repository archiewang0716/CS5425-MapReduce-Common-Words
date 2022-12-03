// Matric Number: A0236008M
// Name: Wang Changqin
// TopkCommonWords.java

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.Comparator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 

public class TopkCommonWords {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text word = new Text();
    private Text fileName = new Text();
    private Set<String> stopwords = new HashSet<String>();
    private String FileName = new String();
    protected void setup(Context context) {
      Configuration conf = context.getConfiguration();
      try {
        // Path path = new Path("data/stopwords.txt");
        Path path = new Path(conf.get("stopwords_path"));
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String word = null;
        while((word = br.readLine()) != null) {
          stopwords.add(word);
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      

    }
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      FileSplit fs = (FileSplit)context.getInputSplit();
      FileName = fs.getPath().getName();
      while (itr.hasMoreTokens()) {
        String tmp = itr.nextToken();

        if (!stopwords.contains(tmp)) {
          word.set(tmp);
          fileName.set(FileName);
          context.write(word, fileName);
        }
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,IntWritable,Text> {
    private IntWritable result = new IntWritable();
    private String fileName = new String();
    private Map<Integer, String> map = new TreeMap<Integer, String>(new Comparator<Integer>() {
      @Override
      public int compare(Integer x, Integer y) {
        return y.compareTo(x);
      }
    });
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      int sum1 = 0;
      int sum2 = 0;
      for (Text val : values) {
        fileName = val.toString();
        if (fileName.equals("task1-input1.txt")) {
          sum1 += 1;
        } else if (fileName.equals("task1-input2.txt")) {
          sum2 += 1;
        }
      }
      int min_res = Math.min(sum1, sum2);
      if (min_res != 0) {
        if (map.containsKey(min_res)) {
          map.put(min_res, map.get(min_res) + "\t" + key.toString());
        }
        else {
          map.put(min_res, key.toString());
        }
      }
    }
    public void cleanup(Context context) throws IOException, InterruptedException {
      int i = 0;
      for (Integer key: map.keySet()) {
        if (i >= 20) {
          break;
        }
        for (String word_str: map.get(key).split("\t")) {
          if (i >= 20) {
            break;
          }
          i++;
          int res = key;
          Text word = new Text(word_str);
          result.set(res);
          context.write(result, word);
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("stopwords_path", args[2]);
    Job job = Job.getInstance(conf, "topk common words");
    job.setJarByClass(TopkCommonWords.class);

    job.setMapperClass(TokenizerMapper.class);
    // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileInputFormat.addInputPath(job, new Path(args[2]));
    FileOutputFormat.setOutputPath(job, new Path(args[3]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
