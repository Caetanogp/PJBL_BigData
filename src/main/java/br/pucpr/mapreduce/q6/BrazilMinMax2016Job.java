package br.pucpr.mapreduce.q6;

import br.pucpr.mapreduce.common.CsvParser;
import br.pucpr.mapreduce.common.MinMaxWritable;
import br.pucpr.mapreduce.common.Transaction;

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

// Questao 6 - Transacao mais cara e mais barata no Brasil em 2016.
// Usa Combiner e o Writable customizado MinMaxWritable.
public class BrazilMinMax2016Job {

    // Mapper: filtra Brasil em 2016 e emite (Brazil_2016, MinMaxWritable(price, price))
    public static class BrazilPriceMapper extends Mapper<LongWritable, Text, Text, MinMaxWritable> {

        private Text chave = new Text("Brazil_2016");
        private MinMaxWritable valor = new MinMaxWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            Transaction tx = CsvParser.parse(value.toString());
            if (tx == null) {
                return;
            }

            // Filtra somente Brasil
            if (!tx.getCountry().equalsIgnoreCase("Brazil")) {
                return;
            }

            try {
                int ano = Integer.parseInt(tx.getYear());
                if (ano != 2016) {
                    return;
                }
                double preco = Double.parseDouble(tx.getPrice());
                // Inicializa min e max iguais ao proprio preco
                valor.set(preco, preco);
                context.write(chave, valor);
            } catch (NumberFormatException e) {
                // ignora linhas com numeros invalidos
            }
        }
    }

    // Reducer (e Combiner) que descobre o menor min e o maior max
    public static class MinMaxReducer extends Reducer<Text, MinMaxWritable, Text, MinMaxWritable> {

        @Override
        protected void reduce(Text key, Iterable<MinMaxWritable> values, Context context)
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
            System.err.println("Uso: BrazilMinMax2016Job <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Q6 - Brazil Min Max Price 2016");
        job.setJarByClass(BrazilMinMax2016Job.class);

        job.setMapperClass(BrazilPriceMapper.class);
        job.setCombinerClass(MinMaxReducer.class);
        job.setReducerClass(MinMaxReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MinMaxWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
