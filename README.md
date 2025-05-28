# Product Co-Occurrence Analysis

## 📌Overview
This project analyzes **product co-occurrences** within shopping baskets. It reads a large CSV file, processes data **efficiently in chunks**, and **computes product pair frequencies**—showing how often two products are bought together.

## 🚀 Features
**Dynamic CSV Handling**: Works with or without headers.  
**Chunked Processing**: Efficient for large datasets.  
**Memory-Constrained Execution**: Handles large files without exceeding system limits.  
**Product Pair Frequency Calculation**: Computes occurrences of products bought together.  
**Logging**: Tracks processing steps for debugging.  

## 🛠️ Dependencies
Ensure the following Python packages are installed:
```bash
pip install pandas psutil




Assigment_1/
│── data_example.csv             # Input dataset
│── cooccurrence_results.csv     # Output file with product pair counts
│── product_cooccurrence.log     # Log file tracking execution
│── main.py     # Main script
│── README.md                    # Documentation

