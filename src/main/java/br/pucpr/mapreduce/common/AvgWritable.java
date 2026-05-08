package br.pucpr.mapreduce.common;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Writable customizado usado para calcular media (guarda a soma e a contagem)
// Usado nas questoes 5 e 7
public class AvgWritable implements Writable {

    private double sum;
    private long count;

    public AvgWritable() {
        this.sum = 0;
        this.count = 0;
    }

    public AvgWritable(double sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    public double getSum() { return sum; }
    public long getCount() { return count; }

    public void set(double sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    // Calcula a media (soma / contagem)
    public double getAverage() {
        if (count == 0) {
            return 0;
        }
        return sum / count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(sum);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.sum = in.readDouble();
        this.count = in.readLong();
    }

    @Override
    public String toString() {
        return String.format("%.2f", getAverage());
    }
}
