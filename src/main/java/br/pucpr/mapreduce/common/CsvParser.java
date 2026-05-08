package br.pucpr.mapreduce.common;

// Classe utilitaria para fazer o parsing das linhas do CSV
public class CsvParser {

    // Recebe uma linha do CSV e devolve um Transaction.
    // Devolve null se a linha for invalida ou se for o cabecalho.
    public static Transaction parse(String line) {

        if (line == null || line.isEmpty()) {
            return null;
        }

        // Separa os campos pelo ; (mantendo campos vazios no final)
        String[] cols = line.split(";", -1);

        // O CSV precisa ter exatamente 10 colunas
        if (cols.length != 10) {
            return null;
        }

        // Ignora a linha do cabecalho
        if (cols[0].trim().equalsIgnoreCase("Country")) {
            return null;
        }

        // Cria o objeto Transaction com todos os campos limpos (sem espacos)
        return new Transaction(
            cols[0].trim(),
            cols[1].trim(),
            cols[2].trim(),
            cols[3].trim(),
            cols[4].trim(),
            cols[5].trim(),
            cols[6].trim(),
            cols[7].trim(),
            cols[8].trim(),
            cols[9].trim()
        );
    }
}
