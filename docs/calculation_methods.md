# NoiseMap — Calculation Methods & Data Processing Documentation

This document describes the mathematical formulas, data transformations, SQL logic,
and computational steps used throughout the NoiseMap project.

It covers:

1. What LAeq means and how it is calculated  
2. How daily (day_peak) and global (global_peak) peaks are computed  
3. Explanation of SQL CTE pipelines  
4. Preprocessing raw XLSX data  
5. Hourly/daily aggregation formulas  
6. Logic behind generating the HTML noise map  

---

# 1. LAeq — Equivalent Continuous Sound Level

**LAeq** (A-weighted equivalent continuous sound level) is one of the most common measures  
for environmental and road-traffic noise.

It represents:

> The constant sound level that, over a given period,  
> contains the same total energy as the actual varying sound.

### Formula

For *N* measurements with instantaneous sound levels \( L_i \) (in dB):

\[
LAeq = 10 \cdot \log_{10}
\left( \frac{1}{N} \sum_{i=1}^{N} 10^{L_i/10} \right)
\]

This formula is applied implicitly by the measurement equipment  
and **already present in the XLSX dataset**, so the project **does NOT recompute LAeq from raw amplitudes**.

---

# 2. Peak Calculations (day_peak / global_peak)

## 2.1 Definitions

### **Global Peak**
The highest LAeq value observed for a station across the entire dataset:

\[
global\_peak = \max(LAeq_{station})
\]

Returned once per station.

---

### **Day Peak**
The highest LAeq value **per day**:

\[
day\_peak(d) = \max(LAeq_{station, \; date=d})
\]

Unlike global peak, this produces one row per day.

---

# 3. SQL CTE Pipeline Explanation

The project uses a structured SQL query with several CTEs.

```sql
WITH hh AS (...),
     day_peak AS (...),
     global_peak AS (...)
SELECT ...
