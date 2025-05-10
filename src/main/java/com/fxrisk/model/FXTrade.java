package com.fxrisk.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.UUID;

/**
 * Represents a foreign exchange trade with risk assessment capabilities.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class FXTrade {
    private String tradeId;
    private String currencyPair;
    private BigDecimal amount;
    private BigDecimal rate;
    private String direction; // "BUY" or "SELL"
    private String trader;
    private String counterparty;
    private LocalDateTime tradeDate;
    private LocalDateTime settlementDate;
    private BigDecimal riskScore;
    private BigDecimal riskThreshold;
    private String status; // "NEW", "EXECUTED", "SETTLED", "CANCELLED"

    // Default constructor for deserialization
    public FXTrade() {
        this.tradeId = UUID.randomUUID().toString();
        this.tradeDate = LocalDateTime.now();
        this.status = "NEW";
    }

    // Constructor with essential fields
    public FXTrade(String currencyPair, BigDecimal amount, BigDecimal rate, 
                  String direction, String trader, String counterparty) {
        this();
        this.currencyPair = currencyPair;
        this.amount = amount;
        this.rate = rate;
        this.direction = direction;
        this.trader = trader;
        this.counterparty = counterparty;
        this.settlementDate = tradeDate.plusDays(2); // T+2 settlement
        calculateRiskScore();
    }

    /**
     * Calculates the risk score for this trade based on various factors.
     * This is a simplified risk model for demonstration purposes.
     */
    public void calculateRiskScore() {
        // Default risk threshold
        this.riskThreshold = new BigDecimal("7.5");
        
        // Start with base risk
        BigDecimal risk = new BigDecimal("5.0");
        
        // Factor 1: Size of the trade
        if (amount.compareTo(new BigDecimal("1000000")) > 0) {
            risk = risk.add(new BigDecimal("2.0"));
        }
        if (amount.compareTo(new BigDecimal("10000000")) > 0) {
            risk = risk.add(new BigDecimal("3.0"));
        }
        
        // Factor 2: Counterparty risk
        if ("UNKNOWN".equals(counterparty) || counterparty == null) {
            risk = risk.add(new BigDecimal("3.0"));
        } else if (counterparty.contains("HIGH_RISK")) {
            risk = risk.add(new BigDecimal("2.5"));
        }
        
        // Factor 3: Currency pair volatility
        if (isVolatilePair(currencyPair)) {
            risk = risk.add(new BigDecimal("1.5"));
        }
        
        // Factor 4: Direction (BUY might be riskier in some scenarios)
        if ("BUY".equals(direction)) {
            risk = risk.add(new BigDecimal("0.5"));
        }
        
        this.riskScore = risk;
    }
    
    /**
     * Determines if a currency pair is considered volatile.
     * In a real system, this would be based on historical data or market conditions.
     */
    private boolean isVolatilePair(String pair) {
        if (pair == null) return true;
        
        // Simplified example - in reality would check volatility metrics
        return pair.contains("BTC") || pair.contains("ETH") || 
               pair.contains("MXN") || pair.contains("TRY") ||
               pair.contains("ZAR") || pair.contains("BRL");
    }
    
    /**
     * Checks if this trade exceeds the risk threshold.
     */
    public boolean isHighRisk() {
        return riskScore != null && riskThreshold != null && 
               riskScore.compareTo(riskThreshold) > 0;
    }

    // Getters and Setters

    public String getTradeId() {
        return tradeId;
    }

    public void setTradeId(String tradeId) {
        this.tradeId = tradeId;
    }

    public String getCurrencyPair() {
        return currencyPair;
    }

    public void setCurrencyPair(String currencyPair) {
        this.currencyPair = currencyPair;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public BigDecimal getRate() {
        return rate;
    }

    public void setRate(BigDecimal rate) {
        this.rate = rate;
    }

    public String getDirection() {
        return direction;
    }

    public void setDirection(String direction) {
        this.direction = direction;
    }

    public String getTrader() {
        return trader;
    }

    public void setTrader(String trader) {
        this.trader = trader;
    }

    public String getCounterparty() {
        return counterparty;
    }

    public void setCounterparty(String counterparty) {
        this.counterparty = counterparty;
    }

    public LocalDateTime getTradeDate() {
        return tradeDate;
    }

    public void setTradeDate(LocalDateTime tradeDate) {
        this.tradeDate = tradeDate;
    }

    public LocalDateTime getSettlementDate() {
        return settlementDate;
    }

    public void setSettlementDate(LocalDateTime settlementDate) {
        this.settlementDate = settlementDate;
    }

    public BigDecimal getRiskScore() {
        return riskScore;
    }

    public void setRiskScore(BigDecimal riskScore) {
        this.riskScore = riskScore;
    }

    public BigDecimal getRiskThreshold() {
        return riskThreshold;
    }

    public void setRiskThreshold(BigDecimal riskThreshold) {
        this.riskThreshold = riskThreshold;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FXTrade fxTrade = (FXTrade) o;
        return Objects.equals(tradeId, fxTrade.tradeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tradeId);
    }

    @Override
    public String toString() {
        return "FXTrade{" +
                "tradeId='" + tradeId + '\'' +
                ", currencyPair='" + currencyPair + '\'' +
                ", amount=" + amount +
                ", direction='" + direction + '\'' +
                ", status='" + status + '\'' +
                ", riskScore=" + riskScore +
                '}';
    }
}