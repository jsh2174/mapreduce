import java.io.IOException;
import java.util.*;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.fs.*;

public class WordCount {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		private Text word = new Text();
		private static LongWritable one = new LongWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, "\t\r\n ,|()<>'");
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken().toLowerCase());
				context.write(word, one);
			}
		}
	}

	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		private LongWritable sumWritable = new LongWritable();

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {

			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}

			sumWritable.set(sum);
			context.write(key, sumWritable);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job( conf, "WordCount" );
			
		// 1. Job Instance�� ������ �ʱ�ȭ �۾�
		job.setJarByClass( WordCount.class );
		// 2. �� Ŭ���� ����
		job.setMapperClass( MyMapper.class );
		// 3. ���ེ Ŭ���� ����
		job.setReducerClass( MyReducer.class );
			
		// 4. ��� Ű Ÿ��
		job.setOutputKeyClass( Text.class);
		// 5. ��� ��� Ÿ��
		job.setMapOutputValueClass( LongWritable.class );
			
		// 6. �Է� ���� ���� ����
		job.setInputFormatClass( TextInputFormat.class );
		// 7. ��� ���� ���� ����
		job.setOutputFormatClass( TextOutputFormat.class );
			
		// 8. �Է� ���� ��ġ ����
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// 9. Ǯ�� ���� ��ġ ����
		FileOutputFormat.setOutputPath(job,  new Path(args[1]));
		
		// 10. ����
		job.waitForCompletion(true);
	}
}
