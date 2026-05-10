package br.pucpr.mapreduce.q1;

import br.pucpr.mapreduce.common.CsvParser;
import br.pucpr.mapreduce.common.Transaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// Questao 1 - Conta o numero de transacoes envolvendo o Brasil
public class BrazilTransactionsJob {

    // Mapper: filtra as linhas do Brasil e emite (Brazil, 1)
    public static class BrazilMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private IntWritable um = new IntWritable(1);
        private Text chaveBrasil = new Text("Brazil");

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Transaction tx = CsvParser.parse(value.toString());
            if (tx == null) {
                return;
            }

            // Filtra apenas transacoes do Brasil
            if (tx.getCountry().equalsIgnoreCase("Brazil")) {
                context.write(chaveBrasil, um);
            }
        }
    }

    // Reducer (e Combiner) que soma os 1's de cada chave
    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int total = 0;
            for (IntWritable v : values) {
                total += v.get();
            }
            context.write(key, new IntWritable(total));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: BrazilTransactionsJob <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Q1 - Brazil Transactions");
        job.setJarByClass(BrazilTransactionsJob.class);

        job.setMapperClass(BrazilMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
