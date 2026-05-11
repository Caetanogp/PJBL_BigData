package br.pucpr.mapreduce.q5;

import br.pucpr.mapreduce.common.AvgWritable;
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

// Questao 5 - Valor medio das transacoes por ano somente no Brasil.
// Usa o Writable customizado AvgWritable (soma + contagem).
public class BrazilAverageByYearJob {

    // Mapper: para cada linha do Brasil, emite (ano, AvgWritable(price, 1))
    public static class BrazilPriceMapper extends Mapper<LongWritable, Text, IntWritable, AvgWritable> {

        private IntWritable chaveAno = new IntWritable();
        private AvgWritable valor = new AvgWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Transaction tx = CsvParser.parse(value.toString());
            if (tx == null) {
                return;
            }

            // Filtra apenas Brasil
            if (!tx.getCountry().equalsIgnoreCase("Brazil")) {
                return;
            }

            // Tenta converter ano e price. Se falhar, ignora a linha.
            try {
                int ano = Integer.parseInt(tx.getYear());
                double preco = Double.parseDouble(tx.getPrice());
                chaveAno.set(ano);
                valor.set(preco, 1);
                context.write(chaveAno, valor);
            } catch (NumberFormatException e) {
                // ignora linhas com numeros invalidos
            }
        }
    }

    // Combiner: soma parcialmente sum e count para reduzir trafego de rede
    public static class AvgCombiner extends Reducer<IntWritable, AvgWritable, IntWritable, AvgWritable> {

        @Override
        protected void reduce(IntWritable key, Iterable<AvgWritable> values, Context context)
                throws IOException, InterruptedException {

            double soma = 0;
            long contagem = 0;
            for (AvgWritable v : values) {
                soma += v.getSum();
                contagem += v.getCount();
            }
            context.write(key, new AvgWritable(soma, contagem));
        }
    }

    // Reducer: faz a soma final e calcula a media (soma / contagem)
    public static class AvgReducer extends Reducer<IntWritable, AvgWritable, IntWritable, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<AvgWritable> values, Context context)
                throws IOException, InterruptedException {

            double soma = 0;
            long contagem = 0;
            for (AvgWritable v : values) {
                soma += v.getSum();
                contagem += v.getCount();
            }

            double media = 0;
            if (contagem > 0) {
                media = soma / contagem;
            }

            context.write(key, new Text(String.format("%.2f", media)));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: BrazilAverageByYearJob <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Q5 - Brazil Average Price by Year");
        job.setJarByClass(BrazilAverageByYearJob.class);

        job.setMapperClass(BrazilPriceMapper.class);
        job.setCombinerClass(AvgCombiner.class);
        job.setReducerClass(AvgReducer.class);

        // Tipos do Mapper (chave/valor intermediarios)
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(AvgWritable.class);

        // Tipos do Reducer (chave/valor finais)
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
