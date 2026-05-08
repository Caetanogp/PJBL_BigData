package br.pucpr.mapreduce.common;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Writable customizado para guardar o minimo e o maximo de um conjunto de valores
// Usado nas questoes 6 e 8
public class MinMaxWritable implements Writable {

    private double min;
    private double max;

    public MinMaxWritable() {
        this.min = 0;
        this.max = 0;
    }

    public MinMaxWritable(double min, double max) {
        this.min = min;
        this.max = max;
    }

    public double getMin() { return min; }
    public double getMax() { return max; }

    public void set(double min, double max) {
        this.min = min;
        this.max = max;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(min);
        out.writeDouble(max);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.min = in.readDouble();
        this.max = in.readDouble();
    }

    @Override
    public String toString() {
        return String.format("%.2f\t%.2f", min, max);
    }
}
