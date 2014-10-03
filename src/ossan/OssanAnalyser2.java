package ossan;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class OssanAnalyser2 {

	// MapReduceを実行するためのドライバ
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		// MapperクラスとReducerクラスを指定
		Job job = new Job(new Configuration());
		job.setJarByClass(OssanAnalyser2.class);       // ★このファイルのメインクラスの名前
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setJobName("2014004");                   // ★自分の学籍番号

		// 入出力フォーマットをテキストに指定
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// MapperとReducerの出力の型を指定
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 入出力ファイルを指定
		String inputpath = "out/ossan";
		String outputpath = "out/ossan2";     // ★MRの出力先
		if (args.length > 0) {
			inputpath = args[0];
		}

		FileInputFormat.setInputPaths(job, new Path(inputpath));
		FileOutputFormat.setOutputPath(job, new Path(outputpath));

		// 出力フォルダは実行の度に毎回削除する（上書きエラーが出るため）
		PosUtils.deleteOutputDir(outputpath);

		// Reducerで使う計算機数を指定
		job.setNumReduceTasks(8);

		// MapReduceジョブを投げ，終わるまで待つ．
		job.waitForCompletion(true);
	}


	// Mapperクラスのmap関数を定義
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private final int DATE = 0;
		private final int TYPE = 1;
		private final int COUNT = 2;

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// csvファイルをカンマで分割して，配列に格納する
			String csv[] = value.toString().split("\t");
			int payDay = 0;
			int date = 0;

			if(csv[DATE].startsWith("0")) date = Integer.valueOf(csv[DATE].substring(1));
			else date = Integer.valueOf(csv[DATE]);
			// 給料日前か後か

			if((date>=25)||(date<5)) payDay = 1;
			else if((date>=15)&&(date<25)) payDay = 2;
			//if((date>=25)&&(date<28)) payDay = 1;
			//else if((date>=21)&&(date<25)) payDay = 2;
			else payDay = 3;

			// valueとなる値段を取得
			String count = payDay+","+csv[COUNT];

			// keyを取得
			String name = csv[TYPE];

			// emitする （emitデータはCSKVオブジェクトに変換すること）
			context.write(new Text(name), new Text(count));
		}

	}


	// Reducerクラスのreduce関数を定義
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		private final int DATE = 0;
		private final int COUNT = 1;

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// 売り上げを合計
			long countBefore = 0,countAfter = 0,countEtc = 0;
			String bigSabun = "";
			for (Text value : values) {
				String csv[] = value.toString().split(",");
				if(csv[DATE].equals("1")) countAfter += Long.valueOf(csv[COUNT]);
				else if(csv[DATE].equals("2")) countBefore += Long.valueOf(csv[COUNT]);
				else countEtc += Long.valueOf(csv[COUNT]);
			}
			if((countAfter < 1000)&&(countBefore < 1000)) return;//bigSabun = "after = before";
			else if (countAfter >= countBefore*2) bigSabun = "after>>>before";
			else if (countAfter*2 <= countBefore) bigSabun = "after<<<before";
			else if (countAfter >= countBefore*1.6) bigSabun = "after>> before";
			else if (countAfter*1.6 <= countBefore) bigSabun = "after<< before";
			else if (countAfter >= countBefore*1.3) bigSabun = "after > before";
			else if (countAfter*1.3 <= countBefore) bigSabun = "after < before";
			else bigSabun = "after = before";

			String output = countAfter + "\t" + countBefore + "\t" + countEtc;
			// emit
			context.write(new Text(bigSabun + "\t" + key), new Text(output));
		}
	}

}

