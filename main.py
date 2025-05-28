import os
import logging
import pandas as pd
import psutil
from collections import defaultdict
from itertools import combinations

# Configure logging
logging.basicConfig(
    filename="product_cooccurrence.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# File paths (Update if needed)
FILE_PATH = r"C:\Users\Poonam shekhawat\PycharmProjects\Assigment_1\data_1.csv"
OUTPUT_FILE = r"C:\Users\Poonam shekhawat\PycharmProjects\Assigment_1\cooccurrence_results.csv"

# Simulated memory constraint (in MB)
SIMULATED_MEMORY_LIMIT_MB = 50


def get_available_memory() -> float:
    """
    Get the available system memory in megabytes (MB).

    Returns:
        float: Available memory in MB.
    """
    return psutil.virtual_memory().available / (1024 * 1024)


def adjust_chunk_size() -> int:
    """
    Dynamically adjusts the chunk size for reading the CSV based on available memory.

    Returns:
        int: Optimized chunk size for processing.
    """
    available_memory = get_available_memory()
    simulated_available_memory = min(available_memory, SIMULATED_MEMORY_LIMIT_MB)

    if simulated_available_memory < 20:
        return max(100, int(simulated_available_memory / 5))  # Reduce chunk size if critically low
    elif simulated_available_memory < 40:
        return 500  # Medium chunk size for constrained memory
    else:
        return 2000  # Default chunk size for normal execution


def process_chunk(chunk: pd.DataFrame) -> dict:
    """
    Processes a chunk of data to compute product pair co-occurrences within baskets.

    Args:
        chunk (pd.DataFrame): Data containing 'basket_id' and 'product_id'.

    Returns:
        dict: A dictionary with product pairs as keys and occurrence counts as values.
    """
    try:
        logging.info(f"Processing chunk with {len(chunk)} rows before filtering.")

        # Debugging: Print unique product IDs before filtering
        print("Unique product IDs BEFORE filtering:", chunk["product_id"].unique())

        # Handle missing values properly
        chunk["product_id"] = chunk["product_id"].fillna(-1)  # Replace missing values with -1

        # Debugging: Print unique product IDs after fillna
        print("Unique product IDs AFTER fillna:", chunk["product_id"].unique())

        # Filter out invalid product IDs
        chunk = chunk[chunk["product_id"] > 0]

        # Group products by basket_id
        basket_dict = defaultdict(set)
        for basket_id, products in chunk.groupby("basket_id")["product_id"]:
            basket_dict[basket_id].update(products.tolist())

        # Compute product pairs within baskets
        pair_count = defaultdict(int)
        for products in basket_dict.values():
            sorted_products = sorted(products)
            print("Sorted product IDs:", sorted_products)  # Debugging sorted products

            for i in range(len(sorted_products)):
                for j in range(i + 1, len(sorted_products)):
                    pair_count[(sorted_products[i], sorted_products[j])] += 1

        logging.info(f"Chunk processed successfully with {len(basket_dict)} baskets.")
        return pair_count

    except Exception as e:
        logging.error(f"Error processing chunk: {str(e)}")
        return {}


def merge_results(results_list: list) -> dict:
    """
    Merges results from multiple chunks into a final output dictionary.

    Args:
        results_list (list): List of dictionaries containing product pair counts.

    Returns:
        dict: Combined dictionary of product pair occurrences.
    """
    try:
        merged_counts = defaultdict(int)
        for result in results_list:
            for pair, count in result.items():
                merged_counts[pair] += count

        logging.info(f"Merged results successfully with {len(merged_counts)} unique product pairs.")
        return merged_counts

    except Exception as e:
        logging.error(f"Error merging results: {str(e)}")
        return {}


def process_csv_data(file_path: str):
    """
    Reads CSV file sequentially, validates data, computes product co-occurrences, and saves results.

    Args:
        file_path (str): Path to the input CSV file.
    """
    if not os.path.exists(file_path):
        logging.error(f"Error: File '{file_path}' not found.")
        print(f"Error: File '{file_path}' not found.")
        return

    if os.stat(file_path).st_size == 0:
        logging.error(f"Error: File '{file_path}' is empty.")
        print(f"Error: File '{file_path}' is empty.")
        return

    chunk_size = adjust_chunk_size()
    product_pairs_count = defaultdict(int)
    is_file_empty = True  # Track whether any data is processed

    try:
        # Read CSV in chunks (Single Read Operation)
        for chunk in pd.read_csv(
            file_path,
            names=["basket_id", "product_id"],
            dtype={"basket_id": str},
            na_values=["", " ", "NA"],
            chunksize=chunk_size
        ):
            print(f"\nProcessing chunk of size: {chunk_size}")
            print(chunk.head())

            if not chunk.empty:
                is_file_empty = False
                basket_groups = chunk.groupby("basket_id")["product_id"].apply(list)

                for products in basket_groups:
                    for pair in combinations(sorted(products), 2):
                        product_pairs_count[pair] += 1

        if is_file_empty:
            logging.error("Error: Input file is empty.")
            print("Error: Input file is empty. Exiting program.")
            return

        logging.info("Processed product co-occurrences successfully.")

        if product_pairs_count:
            result_df = pd.DataFrame([
                {"product_1": pair[0], "product_2": pair[1], "baskets": count}
                for pair, count in sorted(product_pairs_count.items())
            ])

            # Explicitly remove unwanted product ID = 0
            result_df = result_df[result_df["product_1"] > 0]

            result_df.to_csv(OUTPUT_FILE, index=False)
            logging.info(f"Final results saved to: {OUTPUT_FILE}")
            print(f"\nFinal results saved to: {OUTPUT_FILE}")

    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        print(f"Error: Unexpected issue - {str(e)}")


# Run function on actual CSV data
process_csv_data(FILE_PATH)
