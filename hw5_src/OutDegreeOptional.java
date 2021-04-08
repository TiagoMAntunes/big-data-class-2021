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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

public class OutDegreeOptional
{

	public static class OutDegreeMapper 
		extends Mapper<Object, Text, Text, IntWritable>
	{
    
		private final static IntWritable one = new IntWritable(1);
		private Text node = new Text();
		public void map(Object key, Text value, Context context
							) throws IOException, InterruptedException 
		{
			StringTokenizer itr = new StringTokenizer(value.toString(), "\\n");
			while (itr.hasMoreTokens()) {
				node.set(itr.nextToken().split(" ")[1]); // get start node
				context.write(node, one);
			}
		}
	}
  
	public static class OutDegreeReducer 
		extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, 
									Context context
							) throws IOException, InterruptedException 
		{
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class SortMapper
		extends Mapper<Object, Text, Text, Text>
	{
		private Text node = new Text();
		private Text quantity = new Text();


		public void map(Object key, Text value, Context context
							) throws IOException, InterruptedException 
		{
			StringTokenizer itr = new StringTokenizer(value.toString(), "\\n");
			while (itr.hasMoreTokens()) {
				String[] res = itr.nextToken().split("\\t");
				node.set(res[0]);
				quantity.set(res[1]);
				context.write(quantity, node);
			}
		}
	}
  
	public static class SortReducer 
		extends Reducer<Text, Text, Text, Text> 
	{
		private Text result = new Text();
		private int count = 0;

		public void reduce(Text key, Iterable<Text> values, 
									Context context
							) throws IOException, InterruptedException 
		{
			int max = Integer.parseInt(context.getConfiguration().get("topk"));
			
			for (Text val : values) {
				if (count >= max)
					break;
				result.set(val);
				context.write(result, key);
				count++;
			}
		}
	}

	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
			super(Text.class, true);
		}

		public int compare(WritableComparable w1, WritableComparable w2) {
			int k1 = Integer.parseInt(((Text) w1).toString());
			int k2 = Integer.parseInt(((Text) w2).toString());
			return k2 - k1;
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 4) 
		{
			System.err.println("Usage: outdegree <in>+ <out1> <top_k> <out2>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "outdegree");
		job.setJarByClass(OutDegreeOptional.class);
		job.setMapperClass(OutDegreeMapper.class);
		job.setCombinerClass(OutDegreeReducer.class);
		job.setReducerClass(OutDegreeReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		for (int i = 0; i < otherArgs.length - 3; ++i) 
		{
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,
				new Path(otherArgs[otherArgs.length - 3]));
		if (!job.waitForCompletion(true)) {
			System.exit(1);
		};

		// get top k 
		conf.set("topk", otherArgs[otherArgs.length - 2]);
		job = new Job(conf, "outdegree");
		job.setJarByClass(OutDegreeOptional.class);
		job.setMapperClass(SortMapper.class);
		job.setCombinerClass(SortReducer.class);
		job.setReducerClass(SortReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setSortComparatorClass(KeyComparator.class);
		
		// input is last map's out
		FileInputFormat.addInputPath(job, new Path(otherArgs[otherArgs.length  - 3]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

