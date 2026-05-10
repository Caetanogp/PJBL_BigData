package br.pucpr.mapreduce.q4;

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

// Questao 4 - Conta o numero de transacoes por tipo de fluxo (Export/Import)
public class TransactionsByFlowJob {

    // Mapper: emite (flow, 1) para cada linha valida
    public static class FlowMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private IntWritable um = new IntWritable(1);
        private Text chaveFluxo = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Transaction tx = CsvParser.parse(value.toString());
            if (tx == null) {
                return;
            }

            String flow = tx.getFlow();

            // Ignora linhas com flow vazio
            if (flow.isEmpty()) {
                return;
            }

            chaveFluxo.set(flow);
            context.write(chaveFluxo, um);
        }
    }

    // Reducer (e Combiner) que soma os 1's de cada flow
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
            System.err.println("Uso: TransactionsByFlowJob <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Q4 - Transactions by Flow");
        job.setJarByClass(TransactionsByFlowJob.class);

        job.setMapperClass(FlowMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
