package br.pucpr.mapreduce.common;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Chave composta usada na questao 8 para agrupar por (ano, pais).
// Implementa WritableComparable para que o Hadoop consiga ordenar.
public class YearCountryKey implements WritableComparable<YearCountryKey> {

    private int year;
    private String country;

    public YearCountryKey() {
        this.year = 0;
        this.country = "";
    }

    public YearCountryKey(int year, String country) {
        this.year = year;
        this.country = country;
    }

    public int getYear() { return year; }
    public String getCountry() { return country; }

    public void set(int year, String country) {
        this.year = year;
        this.country = country;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        Text.writeString(out, country);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.country = Text.readString(in);
    }

    @Override
    public int compareTo(YearCountryKey outra) {
        // Primeiro compara o ano, depois o pais
        if (this.year != outra.year) {
            return Integer.compare(this.year, outra.year);
        }
        return this.country.compareTo(outra.country);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof YearCountryKey)) return false;
        YearCountryKey outra = (YearCountryKey) o;
        return year == outra.year && country.equals(outra.country);
    }

    @Override
    public int hashCode() {
        return year * 31 + country.hashCode();
    }

    @Override
    public String toString() {
        return year + "\t" + country;
    }
}
