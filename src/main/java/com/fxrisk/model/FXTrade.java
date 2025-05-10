package com.fxrisk.model;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import java.math.BigDecimal;
import java.util.*;

public class FXTrade {
    // Original properties
    private String id;
    private String currencyPair;
    private BigDecimal amount;
    private String direction;
    private String counterparty;
    private boolean settlementRiskCovered;
    
    // New properties to match FXRiskMonitoringApp usage
    private String tradeId;
    private String baseCurrency;
    private String counterCurrency;
    private BigDecimal rate;
    private String trader;
    private String tradeDate;
    private String valueDate;
    
    // Thresholds for amount risk assessment
    private static final BigDecimal MEDIUM_AMOUNT_THRESHOLD = new BigDecimal("5000000");
    private static final BigDecimal HIGH_AMOUNT_THRESHOLD = new BigDecimal("10000000");
    
    // Thresholds for risk score categorization
    private static final int MEDIUM_RISK_THRESHOLD = 3;
    private static final int HIGH_RISK_THRESHOLD = 6;
    
    // Volatile currency pairs with higher risk - use unmodifiable to prevent accidental modification
    private static final Set<String> HIGH_RISK_CURRENCIES;
    
    // Medium risk counterparties
    private static final Set<String> MEDIUM_RISK_COUNTERPARTIES;
    
    // High risk counterparties
    private static final Set<String> HIGH_RISK_COUNTERPARTIES;
    
    static {
        Set<String> highRiskCurrencies = new HashSet<>();
        highRiskCurrencies.add("USD/RUB");
        highRiskCurrencies.add("EUR/TRY");
        highRiskCurrencies.add("USD/TRY");
        highRiskCurrencies.add("GBP/TRY");
        highRiskCurrencies.add("USD/BRL");
        highRiskCurrencies.add("USD/ZAR");
        HIGH_RISK_CURRENCIES = Collections.unmodifiableSet(highRiskCurrencies);
        
        Set<String> mediumRiskCounterparties = new HashSet<>();
        mediumRiskCounterparties.add("BANK_B");
        mediumRiskCounterparties.add("HEDGE_FUND_A");
        mediumRiskCounterparties.add("BROKER_C");
        MEDIUM_RISK_COUNTERPARTIES = Collections.unmodifiableSet(mediumRiskCounterparties);
        
        Set<String> highRiskCounterparties = new HashSet<>();
        highRiskCounterparties.add("BANK_C");
        highRiskCounterparties.add("HEDGE_FUND_B");
        highRiskCounterparties.add("OFFSHORE_ENTITY_A");
        HIGH_RISK_COUNTERPARTIES = Collections.unmodifiableSet(highRiskCounterparties);
    }

    /**
     * Default constructor for creating an empty FXTrade, to be populated with setters.
     */
    public FXTrade() {
        // Default constructor for creating an empty object
    }

    /**
     * Full constructor with original properties
     */
    public FXTrade(String id, String currencyPair, BigDecimal amount, String direction, 
                  String counterparty, boolean settlementRiskCovered) {
        // Validate inputs
        this.id = Objects.requireNonNull(id, "Trade ID cannot be null");
        this.currencyPair = Objects.requireNonNull(currencyPair, "Currency pair cannot be null");
        this.amount = Objects.requireNonNull(amount, "Amount cannot be null");
        
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Trade amount must be positive");
        }
        
        this.direction = Objects.requireNonNull(direction, "Direction cannot be null");
        this.counterparty = Objects.requireNonNull(counterparty, "Counterparty cannot be null");
        this.settlementRiskCovered = settlementRiskCovered;
    }
    
    /**
     * Constructor with new property set
     */
    public FXTrade(String tradeId, String baseCurrency, String counterCurrency, BigDecimal amount,
                  BigDecimal rate, String trader, String tradeDate, String valueDate) {
        this.tradeId = Objects.requireNonNull(tradeId, "Trade ID cannot be null");
        this.baseCurrency = Objects.requireNonNull(baseCurrency, "Base currency cannot be null");
        this.counterCurrency = Objects.requireNonNull(counterCurrency, "Counter currency cannot be null");
        this.amount = Objects.requireNonNull(amount, "Amount cannot be null");
        this.rate = Objects.requireNonNull(rate, "Rate cannot be null");
        this.trader = Objects.requireNonNull(trader, "Trader cannot be null");
        this.tradeDate = Objects.requireNonNull(tradeDate, "Trade date cannot be null");
        this.valueDate = Objects.requireNonNull(valueDate, "Value date cannot be null");
        
        // Set the currencyPair based on base and counter currency for risk calculation
        this.currencyPair = baseCurrency + "/" + counterCurrency;
        // Default values for other properties
        this.id = tradeId;
        this.direction = "BUY";  // Default direction
        this.counterparty = "UNKNOWN";  // Default counterparty
        this.settlementRiskCovered = false;  // Default settlement risk
    }
    
    public RiskLevel calculateRiskScore() {
        int riskScore = 0;
        
        // Check currency pair risk
        if (HIGH_RISK_CURRENCIES.contains(currencyPair)) {
            riskScore += 3;
        }
        
        // Check amount risk
        if (amount.compareTo(MEDIUM_AMOUNT_THRESHOLD) >= 0 && 
            amount.compareTo(HIGH_AMOUNT_THRESHOLD) < 0) {
            riskScore += 2;
        } else if (amount.compareTo(HIGH_AMOUNT_THRESHOLD) >= 0) {
            riskScore += 4;
        }
        
        // Check counterparty risk
        if (HIGH_RISK_COUNTERPARTIES.contains(counterparty)) {
            riskScore += 3;
        } else if (MEDIUM_RISK_COUNTERPARTIES.contains(counterparty)) {
            riskScore += 2;
        }
        
        // Settlement risk mitigation
        if (!settlementRiskCovered) {
            riskScore += 1;
        }
        
        return getRiskLevelFromScore(riskScore);
    }
    
    private RiskLevel getRiskLevelFromScore(int riskScore) {
        if (riskScore >= HIGH_RISK_THRESHOLD) {
            return RiskLevel.HIGH;
        } else if (riskScore >= MEDIUM_RISK_THRESHOLD) {
            return RiskLevel.MEDIUM;
        } else {
            return RiskLevel.LOW;
        }
    }

    // Original getters
    public String getId() {
        return id;
    }

    public String getCurrencyPair() {
        return currencyPair;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getDirection() {
        return direction;
    }

    public String getCounterparty() {
        return counterparty;
    }

    public boolean isSettlementRiskCovered() {
        return settlementRiskCovered;
    }
    
    // New getters for additional properties
    public String getTradeId() {
        return tradeId;
    }
    
    public String getBaseCurrency() {
        return baseCurrency;
    }
    
    public String getCounterCurrency() {
        return counterCurrency;
    }
    
    public BigDecimal getRate() {
        return rate;
    }
    
    public String getTrader() {
        return trader;
    }
    
    public String getTradeDate() {
        return tradeDate;
    }
    
    public String getValueDate() {
        return valueDate;
    }
    
    // Original setters
    public void setId(String id) {
        this.id = id;
    }
    
    public void setCurrencyPair(String currencyPair) {
        this.currencyPair = currencyPair;
    }
    
    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }
    
    public void setDirection(String direction) {
        this.direction = direction;
    }
    
    public void setCounterparty(String counterparty) {
        this.counterparty = counterparty;
    }
    
    public void setSettlementRiskCovered(boolean settlementRiskCovered) {
        this.settlementRiskCovered = settlementRiskCovered;
    }
    
    // New setters for additional properties
    public void setTradeId(String tradeId) {
        this.tradeId = tradeId;
        // Also update the original id for consistency
        this.id = tradeId;
    }
    
    public void setBaseCurrency(String baseCurrency) {
        this.baseCurrency = baseCurrency;
        // Update currency pair if both currencies are set
        if (this.counterCurrency != null) {
            this.currencyPair = baseCurrency + "/" + this.counterCurrency;
        }
    }
    
    public void setCounterCurrency(String counterCurrency) {
        this.counterCurrency = counterCurrency;
        // Update currency pair if both currencies are set
        if (this.baseCurrency != null) {
            this.currencyPair = this.baseCurrency + "/" + counterCurrency;
        }
    }
    
    public void setRate(BigDecimal rate) {
        this.rate = rate;
    }
    
    public void setTrader(String trader) {
        this.trader = trader;
    }
    
    public void setTradeDate(String tradeDate) {
        this.tradeDate = tradeDate;
    }
    
    public void setValueDate(String valueDate) {
        this.valueDate = valueDate;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FXTrade fxTrade = (FXTrade) o;
        return settlementRiskCovered == fxTrade.settlementRiskCovered &&
               Objects.equals(id, fxTrade.id) &&
               Objects.equals(tradeId, fxTrade.tradeId) &&
               Objects.equals(currencyPair, fxTrade.currencyPair) &&
               Objects.equals(baseCurrency, fxTrade.baseCurrency) &&
               Objects.equals(counterCurrency, fxTrade.counterCurrency) &&
               Objects.equals(amount, fxTrade.amount) &&
               Objects.equals(rate, fxTrade.rate) &&
               Objects.equals(direction, fxTrade.direction) &&
               Objects.equals(trader, fxTrade.trader) &&
               Objects.equals(counterparty, fxTrade.counterparty) &&
               Objects.equals(tradeDate, fxTrade.tradeDate) &&
               Objects.equals(valueDate, fxTrade.valueDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, tradeId, currencyPair, baseCurrency, counterCurrency, 
                           amount, rate, direction, trader, counterparty, 
                           tradeDate, valueDate, settlementRiskCovered);
    }

    @Override
    public String toString() {
        return "FXTrade{" +
               "id='" + id + '\'' +
               ", tradeId='" + tradeId + '\'' +
               ", currencyPair='" + currencyPair + '\'' +
               ", baseCurrency='" + baseCurrency + '\'' +
               ", counterCurrency='" + counterCurrency + '\'' +
               ", amount=" + amount +
               ", rate=" + rate +
               ", direction='" + direction + '\'' +
               ", trader='" + trader + '\'' +
               ", counterparty='" + counterparty + '\'' +
               ", tradeDate='" + tradeDate + '\'' +
               ", valueDate='" + valueDate + '\'' +
               ", settlementRiskCovered=" + settlementRiskCovered +
               '}';
    }
    
}
