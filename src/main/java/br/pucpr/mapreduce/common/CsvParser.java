package br.pucpr.mapreduce.common;

  // Classe utilitaria para fazer o parsing das linhas do CSV
  public class CsvParser {

      private static String norm(String s) {
          if (s == null) return "";
          String v = s.trim();

          // remove BOM UTF-8 se vier no inicio do campo
          if (!v.isEmpty() && v.charAt(0) == '\uFEFF') {
              v = v.substring(1);
          }

          // remove aspas externas: "valor" -> valor
          if (v.length() >= 2 && v.startsWith("\"") && v.endsWith("\"")) {
              v = v.substring(1, v.length() - 1).trim();
          }

          return v;
      }

      // Recebe uma linha do CSV e devolve um Transaction.
      // Devolve null se a linha for invalida ou se for cabecalho.
      public static Transaction parse(String line) {

          if (line == null || line.trim().isEmpty()) {
              return null;
          }

          // remove BOM UTF-8 no inicio da linha, se existir
          if (!line.isEmpty() && line.charAt(0) == '\uFEFF') {
              line = line.substring(1);
          }

          // separa os campos por ';' mantendo vazios no final
          String[] cols = line.split(";", -1);

          // o CSV precisa ter exatamente 10 colunas
          if (cols.length != 10) {
              return null;
          }

          String country = norm(cols[0]);
          String year = norm(cols[1]);
          String commodityCode = norm(cols[2]);
          String commodity = norm(cols[3]);
          String flow = norm(cols[4]);
          String price = norm(cols[5]);
          String weight = norm(cols[6]);
          String unit = norm(cols[7]);
          String amount = norm(cols[8]);
          String category = norm(cols[9]);

          // ignora cabecalho em formatos diferentes de dataset
          String c0 = country.toLowerCase();
          String c1 = year.toLowerCase();
          String c2 = commodityCode.toLowerCase();
          String c3 = commodity.toLowerCase();
          String c4 = flow.toLowerCase();
          String c9 = category.toLowerCase();

          boolean isHeader =
                  (c0.equals("country") || c0.equals("country_or_area"))
                          && c1.equals("year")
                          && (c2.equals("commodity code") || c2.equals("comm_code"))
                          && c3.equals("commodity")
                          && c4.equals("flow")
                          && c9.equals("category");

          if (isHeader) {
              return null;
          }

          // monta a transacao com campos normalizados
          return new Transaction(
                  country,
                  year,
                  commodityCode,
                  commodity,
                  flow,
                  price,
                  weight,
                  unit,
                  amount,
                  category
          );
      }
  }




