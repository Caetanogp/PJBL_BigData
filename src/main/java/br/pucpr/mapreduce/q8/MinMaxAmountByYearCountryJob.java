package br.pucpr.mapreduce.q8;

import br.pucpr.mapreduce.common.CsvParser;
import br.pucpr.mapreduce.common.MinMaxWritable;
import br.pucpr.mapreduce.common.Transaction;
import br.pucpr.mapreduce.common.YearCountryKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// Questao 8 - Maior e menor preco com base na coluna Amount por ano e pais.
// Usa o WritableComparable YearCountryKey como chave composta e Combiner.
public class MinMaxAmountByYearCountryJob {

    // Mapper: emite ((ano, pais), MinMaxWritable(amount, amount)) para cada linha valida
    public static class AmountMapper extends Mapper<LongWritable, Text, YearCountryKey, MinMaxWritable> {

        private YearCountryKey chave = new YearCountryKey();
        private MinMaxWritable valor = new MinMaxWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Transaction tx = CsvParser.parse(value.toString());
            if (tx == null) {
                return;
            }

            String pais = tx.getCountry();

            // Ignora linhas sem pais
            if (pais.isEmpty()) {
                return;
            }

            try {
                int ano = Integer.parseInt(tx.getYear());
                double quantidade = Double.parseDouble(tx.getAmount());
                chave.set(ano, pais);
                valor.set(quantidade, quantidade);
                context.write(chave, valor);
            } catch (NumberFormatException e) {
                // ignora linhas com numeros invalidos
            }
        }
    }

    // Reducer (e Combiner) que descobre o menor min e o maior max para cada (ano, pais)
    public static class MinMaxReducer extends Reducer<YearCountryKey, MinMaxWritable, YearCountryKey, MinMaxWritable> {

        @Override
        protected void reduce(YearCountryKey key, Iterable<MinMaxWritable> values, Context context)
                throws IOException, InterruptedException {

            double menor = 0;
            double maior = 0;
            boolean primeiro = true;

            for (MinMaxWritable v : values) {
                if (primeiro) {
                    menor = v.getMin();
                    maior = v.getMax();
                    primeiro = false;
                } else {
                    if (v.getMin() < menor) {
                        menor = v.getMin();
                    }
                    if (v.getMax() > maior) {
                        maior = v.getMax();
                    }
                }
            }

            context.write(key, new MinMaxWritable(menor, maior));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: MinMaxAmountByYearCountryJob <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Q8 - Min Max Amount by Year and Country");
        job.setJarByClass(MinMaxAmountByYearCountryJob.class);

        job.setMapperClass(AmountMapper.class);
        job.setCombinerClass(MinMaxReducer.class);
        job.setReducerClass(MinMaxReducer.class);

        job.setOutputKeyClass(YearCountryKey.class);
        job.setOutputValueClass(MinMaxWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
