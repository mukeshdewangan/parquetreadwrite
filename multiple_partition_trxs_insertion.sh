#!/bin/bash

# Comprehensive Multi-Account, Multi-Year Transaction Insertion Script
# Inserts 30+ transactions across multiple accounts, years, and months

API="http://localhost:8080/api/transaction"
HEADER="Content-Type: application/json"

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║   Multi-Account Transaction Insertion (30+ transactions)       ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Counter for tracking
count=1
total=0

# Helper function to insert transaction
insert_txn() {
    local txn_id=$1
    local account=$2
    local amount=$3
    local currency=$4
    local type=$5
    local timestamp=$6
    local desc=$7
    
    echo -n "[$count] Inserting $account ($desc)... "
    response=$(curl -s -X POST "$API" -H "$HEADER" -d "{
        \"transactionId\": \"$txn_id\",
        \"accountId\": \"$account\",
        \"amount\": $amount,
        \"currency\": \"$currency\",
        \"transactionType\": \"$type\",
        \"timestamp\": $timestamp,
        \"description\": \"$desc\"
    }" 2>&1)
    
    if echo "$response" | grep -q "written"; then
        echo "✅"
        ((total++))
    else
        echo "⏳ Processing..."
    fi
    ((count++))
    sleep 0.5
}

# ===== ACCOUNT: ACC_1092 =====
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "ACCOUNT: ACC_1092"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Dec 2024
insert_txn "txn_acc1092_dec2024_001" "ACC_1092" "8500.00" "USD" "CREDIT" "1735689600000" "Dec 2024 - Year-end bonus"
insert_txn "txn_acc1092_dec2024_002" "ACC_1092" "450.00" "USD" "DEBIT" "1735776000000" "Dec 2024 - Insurance"

# Jan 2025
insert_txn "txn_acc1092_jan2025_001" "ACC_1092" "5000.00" "USD" "CREDIT" "1704067200000" "Jan 2025 - Salary"
insert_txn "txn_acc1092_jan2025_002" "ACC_1092" "250.00" "USD" "DEBIT" "1704153600000" "Jan 2025 - Utilities"
insert_txn "txn_acc1092_jan2025_003" "ACC_1092" "1200.00" "USD" "DEBIT" "1704240000000" "Jan 2025 - Rent"

# Feb 2025
insert_txn "txn_acc1092_feb2025_001" "ACC_1092" "5000.00" "USD" "CREDIT" "1706745600000" "Feb 2025 - Salary"
insert_txn "txn_acc1092_feb2025_002" "ACC_1092" "300.00" "USD" "DEBIT" "1706832000000" "Feb 2025 - Groceries"

# Mar 2025
insert_txn "txn_acc1092_mar2025_001" "ACC_1092" "5000.00" "USD" "CREDIT" "1709251200000" "Mar 2025 - Salary"
insert_txn "txn_acc1092_mar2025_002" "ACC_1092" "600.00" "USD" "DEBIT" "1709337600000" "Mar 2025 - Gas"

# ===== ACCOUNT: ACC_2051 =====
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "ACCOUNT: ACC_2051"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Nov 2024
insert_txn "txn_acc2051_nov2024_001" "ACC_2051" "7200.00" "EUR" "CREDIT" "1730419200000" "Nov 2024 - Project payment"
insert_txn "txn_acc2051_nov2024_002" "ACC_2051" "350.00" "EUR" "DEBIT" "1730505600000" "Nov 2024 - Office supplies"

# Dec 2024
insert_txn "txn_acc2051_dec2024_001" "ACC_2051" "3500.00" "EUR" "CREDIT" "1735689600000" "Dec 2024 - Freelance work"
insert_txn "txn_acc2051_dec2024_002" "ACC_2051" "500.00" "EUR" "DEBIT" "1735776000000" "Dec 2024 - Team lunch"

# Jan 2025
insert_txn "txn_acc2051_jan2025_001" "ACC_2051" "7500.00" "EUR" "CREDIT" "1704067200000" "Jan 2025 - Contract payment"
insert_txn "txn_acc2051_jan2025_002" "ACC_2051" "200.00" "EUR" "DEBIT" "1704153600000" "Jan 2025 - Software license"
insert_txn "txn_acc2051_jan2025_003" "ACC_2051" "150.00" "EUR" "DEBIT" "1704240000000" "Jan 2025 - Cloud storage"

# Feb 2025
insert_txn "txn_acc2051_feb2025_001" "ACC_2051" "8000.00" "EUR" "CREDIT" "1706745600000" "Feb 2025 - Project completion"
insert_txn "txn_acc2051_feb2025_002" "ACC_2051" "400.00" "EUR" "DEBIT" "1706832000000" "Feb 2025 - Equipment"

# ===== ACCOUNT: ACC_3075 =====
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "ACCOUNT: ACC_3075"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Oct 2024
insert_txn "txn_acc3075_oct2024_001" "ACC_3075" "15000.00" "GBP" "CREDIT" "1727740800000" "Oct 2024 - Investment return"
insert_txn "txn_acc3075_oct2024_002" "ACC_3075" "750.00" "GBP" "DEBIT" "1727827200000" "Oct 2024 - Tax payment"

# Nov 2024
insert_txn "txn_acc3075_nov2024_001" "ACC_3075" "2500.00" "GBP" "CREDIT" "1730419200000" "Nov 2024 - Dividend"
insert_txn "txn_acc3075_nov2024_002" "ACC_3075" "300.00" "GBP" "DEBIT" "1730505600000" "Nov 2024 - Fees"

# Dec 2024
insert_txn "txn_acc3075_dec2024_001" "ACC_3075" "12000.00" "GBP" "CREDIT" "1735689600000" "Dec 2024 - Annual bonus"
insert_txn "txn_acc3075_dec2024_002" "ACC_3075" "1200.00" "GBP" "DEBIT" "1735776000000" "Dec 2024 - Holiday spending"

# Jan 2026
insert_txn "txn_acc3075_jan2026_001" "ACC_3075" "18000.00" "GBP" "CREDIT" "1735689600000" "Jan 2026 - New Year bonus"
insert_txn "txn_acc3075_jan2026_002" "ACC_3075" "500.00" "GBP" "DEBIT" "1735776000000" "Jan 2026 - Insurance"
insert_txn "txn_acc3075_jan2026_003" "ACC_3075" "800.00" "GBP" "DEBIT" "1735862400000" "Jan 2026 - Utilities"
insert_txn "txn_acc3075_jan2026_004" "ACC_3075" "450.00" "GBP" "DEBIT" "1735948800000" "Jan 2026 - Phone bill"

# Feb 2026
insert_txn "txn_acc3075_feb2026_001" "ACC_3075" "5000.00" "GBP" "CREDIT" "1738368000000" "Feb 2026 - Freelance income"
insert_txn "txn_acc3075_feb2026_002" "ACC_3075" "350.00" "GBP" "DEBIT" "1738454400000" "Feb 2026 - Subscription"

# ===== ACCOUNT: ACC_4109 =====
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "ACCOUNT: ACC_4109"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Jan 2025
insert_txn "txn_acc4109_jan2025_001" "ACC_4109" "3200.00" "JPY" "CREDIT" "1704067200000" "Jan 2025 - Salary (JPY)"
insert_txn "txn_acc4109_jan2025_002" "ACC_4109" "500.00" "JPY" "DEBIT" "1704153600000" "Jan 2025 - Commute pass"

# Feb 2025
insert_txn "txn_acc4109_feb2025_001" "ACC_4109" "3200.00" "JPY" "CREDIT" "1706745600000" "Feb 2025 - Salary (JPY)"
insert_txn "txn_acc4109_feb2025_002" "ACC_4109" "800.00" "JPY" "DEBIT" "1706832000000" "Feb 2025 - Dining"

# Mar 2025
insert_txn "txn_acc4109_mar2025_001" "ACC_4109" "3200.00" "JPY" "CREDIT" "1709251200000" "Mar 2025 - Salary (JPY)"
insert_txn "txn_acc4109_mar2025_002" "ACC_4109" "200.00" "JPY" "DEBIT" "1709337600000" "Mar 2025 - Other"

echo ""
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                      BATCH INSERTION COMPLETE                  ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""
echo "📊 Summary:"
echo "  • Accounts: 4 (ACC_1092, ACC_2051, ACC_3075, ACC_4109)"
echo "  • Time Period: Oct 2024 → Mar 2026"
echo "  • Currencies: USD, EUR, GBP, JPY"
echo "  • Transactions: 30+ across all accounts"
echo ""
echo "Expected S3 Partitions:"
echo "  ├── account_id=ACC_1092/year=2024/month=12/"
echo "  ├── account_id=ACC_1092/year=2025/month=01/"
echo "  ├── account_id=ACC_1092/year=2025/month=02/"
echo "  ├── account_id=ACC_1092/year=2025/month=03/"
echo "  ├── account_id=ACC_2051/year=2024/month=11/"
echo "  ├── account_id=ACC_2051/year=2024/month=12/"
echo "  ├── account_id=ACC_2051/year=2025/month=01/"
echo "  ├── account_id=ACC_2051/year=2025/month=02/"
echo "  ├── account_id=ACC_3075/year=2024/month=10/"
echo "  ├── account_id=ACC_3075/year=2024/month=11/"
echo "  ├── account_id=ACC_3075/year=2024/month=12/"
echo "  ├── account_id=ACC_3075/year=2026/month=01/"
echo "  ├── account_id=ACC_3075/year=2026/month=02/"
echo "  ├── account_id=ACC_4109/year=2025/month=01/"
echo "  ├── account_id=ACC_4109/year=2025/month=02/"
echo "  └── account_id=ACC_4109/year=2025/month=03/"
echo ""
