package br.pucpr.mapreduce.q2;

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

// Questao 2 - Conta o numero de transacoes por ano
public class TransactionsByYearJob {

    // Mapper: emite (ano, 1) para cada linha valida
    public static class YearMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private IntWritable um = new IntWritable(1);
        private IntWritable chaveAno = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Transaction tx = CsvParser.parse(value.toString());
            if (tx == null) {
                return;
            }

            // Tenta converter o ano para int. Se falhar, ignora a linha.
            try {
                int ano = Integer.parseInt(tx.getYear());
                chaveAno.set(ano);
                context.write(chaveAno, um);
            } catch (NumberFormatException e) {
                // ignora linhas com ano invalido
            }
        }
    }

    // Reducer (e Combiner) que soma os 1's de cada ano
    public static class SumReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
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
            System.err.println("Uso: TransactionsByYearJob <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Q2 - Transactions by Year");
        job.setJarByClass(TransactionsByYearJob.class);

        job.setMapperClass(YearMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
