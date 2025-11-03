import pandas as pd
import numpy as np
import os

# =====================================================
# Configuration
# =====================================================
BASE_DIR = r"C:\Users\rumucao\OneDrive - Universiteit Utrecht\Bureaublad\SpikingJelly\Group18\Data"
os.makedirs(BASE_DIR, exist_ok=True)

# random seed for reproducibility
np.random.seed(42)

# =====================================================
# Function to generate table R
# =====================================================
def generate_R(rows=10000, cols=6, filename="R_10k_d6.csv"):
    """Generate a synthetic numeric table R with given rows and columns."""
    data = np.random.randint(0, 21, size=(rows, cols))
    df = pd.DataFrame(data, columns=[f"col_{i+1}" for i in range(cols)])
    df.insert(0, "row_id", range(rows))
    
    path = os.path.join(BASE_DIR, filename)
    df.to_csv(path, index=False)
    print(f"✅ Generated {filename} with shape {df.shape}")
    return path

# =====================================================
# Function to generate queries Q
# =====================================================
def generate_Q(num_queries=1000, cols=6, filename="Q_1k.csv"):
    """Generate a CSV file of queries with random equality conditions."""
    queries = []
    for i in range(num_queries):
        n_conditions = np.random.randint(1, 4)  # 1–3 conditions per query
        selected_cols = np.random.choice(range(1, cols + 1), n_conditions, replace=False)
        conds = [f"col_{c}={np.random.randint(0, 21)}" for c in selected_cols]
        queries.append(";".join(conds))

    df = pd.DataFrame({
        "query_id": range(num_queries),
        "conditions": queries
    })
    
    path = os.path.join(BASE_DIR, filename)
    df.to_csv(path, index=False)
    print(f"✅ Generated {filename} with {num_queries} queries")
    return path

# =====================================================
# Main Execution
# =====================================================
if __name__ == "__main__":
    print("🔧 Generating synthetic datasets for project...\n")

    # Small dataset for exact testing
    generate_R(rows=1000, cols=6, filename="R_1k_d6.csv")

    # Medium dataset for performance tests
    generate_R(rows=10000, cols=6, filename="R_10k_d6.csv")

    # Large dataset for scalability tests (optional)
    generate_R(rows=50000, cols=6, filename="R_50k_d6.csv")

    # Generate queries
    generate_Q(num_queries=1000, cols=6, filename="Q_1k.csv")

    print("\n🎉 All synthetic datasets generated successfully!")
