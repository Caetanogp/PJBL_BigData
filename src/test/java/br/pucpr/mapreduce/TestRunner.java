package br.pucpr.mapreduce;

import br.pucpr.mapreduce.common.CsvParser;
import br.pucpr.mapreduce.common.Transaction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Testa a logica das 8 questoes do PjBL01 sem precisar do Hadoop rodando.
 * Usa o mesmo CsvParser e Transaction do projeto principal.
 *
 * Dados de entrada: input/test_sample.csv
 * - 1 linha de cabecalho (deve ser ignorada)
 * - 8 linhas validas
 * - 1 linha malformada (menos de 10 colunas, deve ser ignorada)
 * - 1 linha com preco invalido "abc" (valida para Q1/Q2/Q3/Q4/Q8, invalida para Q5/Q6/Q7)
 *
 * Total de transacoes validas carregadas: 9
 */
public class TestRunner {

    // Contadores globais de testes
    static int passou = 0;
    static int falhou = 0;

    // ----------------------------------------------------------------
    // Metodo de verificacao: compara esperado x obtido e imprime resultado
    // ----------------------------------------------------------------
    static void verificar(String nomeTeste, Object esperado, Object obtido) {
        if (esperado.equals(obtido)) {
            System.out.println("  [PASSOU] " + nomeTeste + " => " + obtido);
            passou++;
        } else {
            System.out.println("  [FALHOU] " + nomeTeste
                    + " | esperado=" + esperado + " | obtido=" + obtido);
            falhou++;
        }
    }

    // Compara doubles com tolerancia de 0.01 (evita problemas de locale e ponto flutuante)
    static void verificarDouble(String nomeTeste, double esperado, double obtido) {
        if (Math.abs(esperado - obtido) < 0.01) {
            System.out.printf("  [PASSOU] %s => %.2f%n", nomeTeste, obtido);
            passou++;
        } else {
            System.out.printf("  [FALHOU] %s | esperado=%.2f | obtido=%.2f%n",
                    nomeTeste, esperado, obtido);
            falhou++;
        }
    }

    // ----------------------------------------------------------------
    // Carrega o CSV e devolve apenas as transacoes validas
    // ----------------------------------------------------------------
    static List<Transaction> carregarCSV(String arquivo) throws IOException {
        List<Transaction> transacoes = new ArrayList<Transaction>();
        BufferedReader br = new BufferedReader(new FileReader(arquivo));
        String linha;
        while ((linha = br.readLine()) != null) {
            Transaction tx = CsvParser.parse(linha);
            if (tx != null) {
                transacoes.add(tx);
            }
        }
        br.close();
        return transacoes;
    }

    // ----------------------------------------------------------------
    // Q1 - Numero de transacoes do Brasil
    // ----------------------------------------------------------------
    static long q1Brasil(List<Transaction> txs) {
        long total = 0;
        for (Transaction tx : txs) {
            if (tx.getCountry().equalsIgnoreCase("Brazil")) {
                total++;
            }
        }
        return total;
    }

    // ----------------------------------------------------------------
    // Q2 - Numero de transacoes por ano
    // ----------------------------------------------------------------
    static Map<Integer, Integer> q2PorAno(List<Transaction> txs) {
        Map<Integer, Integer> mapa = new TreeMap<Integer, Integer>();
        for (Transaction tx : txs) {
            try {
                int ano = Integer.parseInt(tx.getYear());
                int atual = mapa.containsKey(ano) ? mapa.get(ano) : 0;
                mapa.put(ano, atual + 1);
            } catch (NumberFormatException e) {
                // ignora linhas com ano invalido
            }
        }
        return mapa;
    }

    // ----------------------------------------------------------------
    // Q3 - Numero de transacoes por categoria
    // ----------------------------------------------------------------
    static Map<String, Integer> q3PorCategoria(List<Transaction> txs) {
        Map<String, Integer> mapa = new TreeMap<String, Integer>();
        for (Transaction tx : txs) {
            String categoria = tx.getCategory();
            if (!categoria.isEmpty()) {
                int atual = mapa.containsKey(categoria) ? mapa.get(categoria) : 0;
                mapa.put(categoria, atual + 1);
            }
        }
        return mapa;
    }

    // ----------------------------------------------------------------
    // Q4 - Numero de transacoes por tipo de fluxo
    // ----------------------------------------------------------------
    static Map<String, Integer> q4PorFlow(List<Transaction> txs) {
        Map<String, Integer> mapa = new TreeMap<String, Integer>();
        for (Transaction tx : txs) {
            String flow = tx.getFlow();
            if (!flow.isEmpty()) {
                int atual = mapa.containsKey(flow) ? mapa.get(flow) : 0;
                mapa.put(flow, atual + 1);
            }
        }
        return mapa;
    }

    // ----------------------------------------------------------------
    // Q5 - Media de price por ano, apenas Brasil
    // ----------------------------------------------------------------
    static Map<Integer, Double> q5MediaBrasilPorAno(List<Transaction> txs) {
        Map<Integer, double[]> acc = new TreeMap<Integer, double[]>(); // [soma, contagem]
        for (Transaction tx : txs) {
            if (!tx.getCountry().equalsIgnoreCase("Brazil")) {
                continue;
            }
            try {
                int ano = Integer.parseInt(tx.getYear());
                double preco = Double.parseDouble(tx.getPrice());
                double[] v = acc.containsKey(ano) ? acc.get(ano) : new double[]{0, 0};
                v[0] += preco;
                v[1]++;
                acc.put(ano, v);
            } catch (NumberFormatException e) {
                // ignora linhas com year ou price invalido
            }
        }
        Map<Integer, Double> resultado = new TreeMap<Integer, Double>();
        for (Map.Entry<Integer, double[]> e : acc.entrySet()) {
            resultado.put(e.getKey(), e.getValue()[0] / e.getValue()[1]);
        }
        return resultado;
    }

    // ----------------------------------------------------------------
    // Q6 - Min e Max de price no Brasil em 2016
    // ----------------------------------------------------------------
    static double[] q6MinMaxBrasil2016(List<Transaction> txs) {
        double menor = 0;
        double maior = 0;
        boolean primeiro = true;

        for (Transaction tx : txs) {
            if (!tx.getCountry().equalsIgnoreCase("Brazil")) {
                continue;
            }
            try {
                int ano = Integer.parseInt(tx.getYear());
                if (ano != 2016) {
                    continue;
                }
                double preco = Double.parseDouble(tx.getPrice());
                if (primeiro) {
                    menor = preco;
                    maior = preco;
                    primeiro = false;
                } else {
                    if (preco < menor) menor = preco;
                    if (preco > maior) maior = preco;
                }
            } catch (NumberFormatException e) {
                // ignora preco invalido
            }
        }
        return new double[]{menor, maior};
    }

    // ----------------------------------------------------------------
    // Q7 - Media de price das exportacoes do Brasil por ano
    // ----------------------------------------------------------------
    static Map<Integer, Double> q7MediaExportBrasilPorAno(List<Transaction> txs) {
        Map<Integer, double[]> acc = new TreeMap<Integer, double[]>();
        for (Transaction tx : txs) {
            if (!tx.getCountry().equalsIgnoreCase("Brazil")) {
                continue;
            }
            if (!tx.getFlow().equalsIgnoreCase("Export")) {
                continue;
            }
            try {
                int ano = Integer.parseInt(tx.getYear());
                double preco = Double.parseDouble(tx.getPrice());
                double[] v = acc.containsKey(ano) ? acc.get(ano) : new double[]{0, 0};
                v[0] += preco;
                v[1]++;
                acc.put(ano, v);
            } catch (NumberFormatException e) {
                // ignora linhas com year ou price invalido
            }
        }
        Map<Integer, Double> resultado = new TreeMap<Integer, Double>();
        for (Map.Entry<Integer, double[]> e : acc.entrySet()) {
            resultado.put(e.getKey(), e.getValue()[0] / e.getValue()[1]);
        }
        return resultado;
    }

    // ----------------------------------------------------------------
    // Q8 - Min e Max de amount por ano e pais
    // ----------------------------------------------------------------
    static Map<String, double[]> q8MinMaxAmountPorAnoPais(List<Transaction> txs) {
        Map<String, double[]> mapa = new TreeMap<String, double[]>();
        for (Transaction tx : txs) {
            String pais = tx.getCountry();
            if (pais.isEmpty()) {
                continue;
            }
            try {
                int ano = Integer.parseInt(tx.getYear());
                double amount = Double.parseDouble(tx.getAmount());
                // Chave: "ano\tpais" - igual ao toString() do YearCountryKey
                String chave = ano + "\t" + pais;
                if (!mapa.containsKey(chave)) {
                    mapa.put(chave, new double[]{amount, amount});
                } else {
                    double[] v = mapa.get(chave);
                    if (amount < v[0]) v[0] = amount;
                    if (amount > v[1]) v[1] = amount;
                }
            } catch (NumberFormatException e) {
                // ignora linhas com year ou amount invalido
            }
        }
        return mapa;
    }

    // ----------------------------------------------------------------
    // MAIN - roda todos os testes e imprime o relatorio final
    // ----------------------------------------------------------------
    public static void main(String[] args) throws Exception {

        String arquivo = args.length > 0 ? args[0] : "input/test_sample.csv";

        System.out.println("============================================================");
        System.out.println("  TESTES AUTOMATIZADOS - PjBL01 MapReduce");
        System.out.println("  Arquivo: " + arquivo);
        System.out.println("============================================================");
        System.out.println();

        List<Transaction> txs = carregarCSV(arquivo);

        System.out.println(">>> PARSER: transacoes validas carregadas = " + txs.size());
        System.out.println("    (esperado=9: 8 linhas de dados + 1 com preco invalido)");
        verificar("CsvParser total validos", 9, txs.size());
        System.out.println();

        // --------------------------
        // Q1
        // --------------------------
        System.out.println("--- Q1: Numero de transacoes do Brasil ---");
        long q1 = q1Brasil(txs);
        System.out.println("  Brazil = " + q1);
        // 6 linhas Brazil: rows 2,3,4,5,9,11 (row 11 tem price invalido mas Q1 nao precisa de price)
        verificar("Q1 Brazil count", 6L, q1);
        System.out.println();

        // --------------------------
        // Q2
        // --------------------------
        System.out.println("--- Q2: Numero de transacoes por ano ---");
        Map<Integer, Integer> q2 = q2PorAno(txs);
        for (Map.Entry<Integer, Integer> e : q2.entrySet()) {
            System.out.println("  " + e.getKey() + " = " + e.getValue());
        }
        verificar("Q2 ano 2016", 6, q2.get(2016));
        verificar("Q2 ano 2017", 2, q2.get(2017));
        verificar("Q2 ano 2018", 1, q2.get(2018));
        System.out.println();

        // --------------------------
        // Q3
        // --------------------------
        System.out.println("--- Q3: Numero de transacoes por categoria ---");
        Map<String, Integer> q3 = q3PorCategoria(txs);
        for (Map.Entry<String, Integer> e : q3.entrySet()) {
            System.out.println("  " + e.getKey() + " = " + e.getValue());
        }
        verificar("Q3 01_live_animals", 4, q3.get("01_live_animals"));
        verificar("Q3 02_meat", 3, q3.get("02_meat"));
        verificar("Q3 03_dairy", 2, q3.get("03_dairy"));
        System.out.println();

        // --------------------------
        // Q4
        // --------------------------
        System.out.println("--- Q4: Numero de transacoes por flow ---");
        Map<String, Integer> q4 = q4PorFlow(txs);
        for (Map.Entry<String, Integer> e : q4.entrySet()) {
            System.out.println("  " + e.getKey() + " = " + e.getValue());
        }
        verificar("Q4 Export", 6, q4.get("Export"));
        verificar("Q4 Import", 3, q4.get("Import"));
        System.out.println();

        // --------------------------
        // Q5
        // --------------------------
        System.out.println("--- Q5: Media de price por ano (Brasil) ---");
        System.out.println("    (row com price='abc' deve ser ignorada)");
        Map<Integer, Double> q5 = q5MediaBrasilPorAno(txs);
        for (Map.Entry<Integer, Double> e : q5.entrySet()) {
            System.out.printf("  %d = %.2f%n", e.getKey(), e.getValue());
        }
        // 2016: Brazil rows com price valido = 5000+3000+15000 / 3 = 7666.67
        verificarDouble("Q5 media 2016", 7666.67, q5.get(2016));
        // 2017: 4000/1 = 4000.00
        verificarDouble("Q5 media 2017", 4000.00, q5.get(2017));
        // 2018: 8000/1 = 8000.00
        verificarDouble("Q5 media 2018", 8000.00, q5.get(2018));
        System.out.println();

        // --------------------------
        // Q6
        // --------------------------
        System.out.println("--- Q6: Min/Max price Brasil 2016 ---");
        System.out.println("    (row com price='abc' deve ser ignorada)");
        double[] q6 = q6MinMaxBrasil2016(txs);
        System.out.printf("  min=%.2f  max=%.2f%n", q6[0], q6[1]);
        // Brazil 2016 prices validos: 5000, 3000, 15000 -> min=3000 max=15000
        verificarDouble("Q6 min price", 3000.00, q6[0]);
        verificarDouble("Q6 max price", 15000.00, q6[1]);
        System.out.println();

        // --------------------------
        // Q7
        // --------------------------
        System.out.println("--- Q7: Media de price das exportacoes do Brasil por ano ---");
        System.out.println("    (row com price='abc' deve ser ignorada)");
        Map<Integer, Double> q7 = q7MediaExportBrasilPorAno(txs);
        for (Map.Entry<Integer, Double> e : q7.entrySet()) {
            System.out.printf("  %d = %.2f%n", e.getKey(), e.getValue());
        }
        // 2016 Export Brazil prices validos: 5000 + 15000 = 20000 / 2 = 10000.00
        verificarDouble("Q7 media export 2016", 10000.00, q7.get(2016));
        // 2017 Export Brazil: 4000/1 = 4000.00
        verificarDouble("Q7 media export 2017", 4000.00, q7.get(2017));
        // 2018 Export Brazil: 8000/1 = 8000.00
        verificarDouble("Q7 media export 2018", 8000.00, q7.get(2018));
        System.out.println();

        // --------------------------
        // Q8
        // --------------------------
        System.out.println("--- Q8: Min/Max de amount por ano e pais ---");
        System.out.println("    (row com price='abc' ainda tem amount=10 valido, deve ser contada)");
        Map<String, double[]> q8 = q8MinMaxAmountPorAnoPais(txs);
        for (Map.Entry<String, double[]> e : q8.entrySet()) {
            System.out.printf("  %s  ->  min=%.2f  max=%.2f%n",
                    e.getKey(), e.getValue()[0], e.getValue()[1]);
        }
        // Brazil 2016: amounts = 50, 30, 150, 10 -> min=10 max=150
        verificarDouble("Q8 Brazil 2016 min amount", 10.0, q8.get("2016\tBrazil")[0]);
        verificarDouble("Q8 Brazil 2016 max amount", 150.0, q8.get("2016\tBrazil")[1]);
        // Argentina 2016: amount=20 -> min=20 max=20
        verificarDouble("Q8 Argentina 2016 min amount", 20.0, q8.get("2016\tArgentina")[0]);
        verificarDouble("Q8 Argentina 2016 max amount", 20.0, q8.get("2016\tArgentina")[1]);
        // Germany 2016: amount=100 -> min=100 max=100
        verificarDouble("Q8 Germany 2016 min amount", 100.0, q8.get("2016\tGermany")[0]);
        verificarDouble("Q8 Germany 2016 max amount", 100.0, q8.get("2016\tGermany")[1]);
        // Brazil 2017: amount=40 -> min=40 max=40
        verificarDouble("Q8 Brazil 2017 min amount", 40.0, q8.get("2017\tBrazil")[0]);
        verificarDouble("Q8 Brazil 2017 max amount", 40.0, q8.get("2017\tBrazil")[1]);
        // Brazil 2018: amount=80 -> min=80 max=80
        verificarDouble("Q8 Brazil 2018 min amount", 80.0, q8.get("2018\tBrazil")[0]);
        verificarDouble("Q8 Brazil 2018 max amount", 80.0, q8.get("2018\tBrazil")[1]);
        System.out.println();

        // --------------------------
        // Relatorio final
        // --------------------------
        System.out.println("============================================================");
        System.out.printf("  RESULTADO FINAL: %d PASSOU  |  %d FALHOU%n", passou, falhou);
        System.out.println("============================================================");

        if (falhou > 0) {
            System.exit(1);
        }
    }
}
