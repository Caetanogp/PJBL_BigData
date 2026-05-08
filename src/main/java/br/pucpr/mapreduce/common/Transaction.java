package br.pucpr.mapreduce.common;

// Classe que representa uma linha do CSV (uma transacao)
public class Transaction {

    private String country;
    private String year;
    private String commodityCode;
    private String commodity;
    private String flow;
    private String price;
    private String weight;
    private String unit;
    private String amount;
    private String category;

    public Transaction(String country, String year, String commodityCode, String commodity,
                       String flow, String price, String weight, String unit,
                       String amount, String category) {
        this.country = country;
        this.year = year;
        this.commodityCode = commodityCode;
        this.commodity = commodity;
        this.flow = flow;
        this.price = price;
        this.weight = weight;
        this.unit = unit;
        this.amount = amount;
        this.category = category;
    }

    public String getCountry() { return country; }
    public String getYear() { return year; }
    public String getCommodityCode() { return commodityCode; }
    public String getCommodity() { return commodity; }
    public String getFlow() { return flow; }
    public String getPrice() { return price; }
    public String getWeight() { return weight; }
    public String getUnit() { return unit; }
    public String getAmount() { return amount; }
    public String getCategory() { return category; }
}
