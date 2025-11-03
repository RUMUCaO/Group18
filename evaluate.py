# evaluate.py
import pandas as pd
import numpy as np
import argparse
import os
from sklearn.metrics.pairwise import euclidean_distances, cosine_similarity

def load_dataset(path):
    import chardet
    with open(path, "rb") as f:
        raw = f.read(20000)
    enc = chardet.detect(raw)["encoding"]
    df = pd.read_csv(path, sep=",", encoding=enc)
    print(f"[DEBUG] Loaded {path} using encoding={enc}, columns={list(df.columns)}")
    return df



def mean_pairwise_distance(df, columns, metric="euclidean"):
    """Compute mean pairwise distance for given numeric columns."""
    X = df[columns].to_numpy()
    if metric == "euclidean":
        dists = euclidean_distances(X)
    elif metric == "cosine":
        dists = 1 - cosine_similarity(X)
    else:
        raise ValueError("Unsupported metric")
    n = len(X)
    if n < 2:
        return 0
    return np.sum(np.triu(dists, 1)) * 2 / (n * (n - 1))

def jaccard_similarity(setA, setB):
    """Compute Jaccard similarity between two sets."""
    inter = len(setA & setB)
    union = len(setA | setB)
    return inter / union if union > 0 else 0

def evaluate_method(result_path, full_dataset_path, method_name, baseline_path=None, metric="euclidean"):
    """Evaluate one method output file."""
    df_full = load_dataset(full_dataset_path)
    df_res = load_dataset(result_path)

    # feature columns: all except id, pop, importance
    cols = [c for c in df_full.columns if c not in ["row_id", "pop", "importance"]]

    # compute diversity
    diversity = mean_pairwise_distance(df_res, cols, metric=metric)

    # compute importance
    imp_mean = df_res["pop"].mean() if "pop" in df_res.columns else 1.0
    score = imp_mean * diversity

    # compare with baseline (optional)
    jaccard = None
    if baseline_path:
        df_base = load_dataset(baseline_path)
        jaccard = jaccard_similarity(set(df_res["row_id"]), set(df_base["row_id"]))

    return {
        "method": method_name,
        "file": os.path.basename(result_path),
        "rows": len(df_res),
        "imp_mean": imp_mean,
        "diversity": diversity,
        "imp_Rprime": score,
        "jaccard": jaccard
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Evaluate method outputs for Data Intensive Systems project")
    parser.add_argument("--results_dir", type=str, default="data/results/", help="Directory with result CSVs")
    parser.add_argument("--dataset", type=str, default="data/R_10k_d6.csv", help="Full dataset path")
    parser.add_argument("--metric", type=str, default="euclidean", choices=["euclidean", "cosine"])
    parser.add_argument("--baseline", type=str, default=None, help="Optional baseline file (for Jaccard)")
    parser.add_argument("--output", type=str, default="evaluation_summary.csv")

    args = parser.parse_args()

    files = [f for f in os.listdir(args.results_dir) if f.endswith(".csv") and "timing" not in f.lower()]
    results = []
    for f in files:
        method_name = f.split("_")[0]
        res = evaluate_method(os.path.join(args.results_dir, f),
                              args.dataset,
                              method_name,
                              baseline_path=args.baseline,
                              metric=args.metric)
        results.append(res)

    df_eval = pd.DataFrame(results)
    df_eval.to_csv(args.output, index=False)
    print("\n✅ Evaluation summary saved to", args.output)
    print(df_eval)
