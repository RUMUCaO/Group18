============================================================
Finding the Most Valuable Parts of a Dataset — Project README
============================================================

Utrecht University | Data Intensive Systems (2025–2026)
Instructor: Prof. Yannis Velegrakis
Group 18 | Submission: 3 Nov 2025

------------------------------------------------------------
📁 Project Structure
------------------------------------------------------------
(IMPORTANT: directory names must match the instructor’s script)

- Src/              : all source code + README + requirements
- Data/             : datasets (inputs) and results (outputs/)
- Report/           : final report file XX.pdf
- Presentation/     : final slides XX.pptx
- Data/results/     : outputs of all methods (.csv)

------------------------------------------------------------
🧱 Source Files Overview
------------------------------------------------------------
generate_dataset.py   : generates synthetic table R (random integers)
generate_queries.py    : creates query set Q with equality conditions
method1.py             : implements popularity-based selection
method2.py             : implements popularity × diversity (greedy)
evaluate.py            : runs scalability tests across dataset sizes
plots.py               : visualizes runtime, diversity, and imp(R′)
main.py                : entry point — dispatches method1/method2
requirements.txt       : dependency list (PySpark, NumPy, Pandas)

------------------------------------------------------------
🚀 Quick Start (Local Spark)
------------------------------------------------------------

1) Generate synthetic dataset and queries
   ```bash
   python3 Src/generate_dataset.py \
       --rows 20000 --cols 6 --out Data/synthetic_R.csv

   python3 Src/generate_queries.py \
       --num_queries 500 --cols 6 --tightness 2 \
       --in Data/synthetic_R.csv --out Data/synthetic_Q.csv


Quick Start (local Spark):
1) Create synthetic table and queries (adjust sizes as needed):
   spark-submit Src/generate_dataset.py --rows 20000 --cols 6 --out Data/synthetic_R.csv
   spark-submit Src/generate_queries.py --num_queries 500 --cols 6 --tightness 2 --in Data/synthetic_R.csv --out Data/synthetic_Q.csv

2) Run Method 1 (popularity):
   Python3 main.py --method 1 --input Data/synthetic_R.csv --queries Data/synthetic_Q.csv --threshold 500 --output Data/results/method1_output.csv

3) Run Method 2 (pop × diversity, centroid-approx greedy):
python3 Src/main.py --method 2 --input Data/synthetic_R.csv --queries Data/synthetic_Q.csv --threshold 500 --output Data/results/method2_output.csv --distance euclidean --standardize

4) (Optional) Evaluate multiple sizes (toy example):
   spark-submit Src/evaluate.py --base_rows 20000 --scales 1,2,4 --cols 6 --num_queries 500 --threshold 500

Notes
- Method 1 is an exact "top-T by popularity".
- Method 2 adds diversity weighting (distance to centroid), using a greedy Spark-parallel algorithm that recomputes candidate gains each round.
- All inputs/outputs files are CSV; the first line of Table R contains headers: col_0,...,col_{C-1}
- Query file syntax: CSV with columns [q_id, conds]; conds is semicolon-separated clauses like "col_0=12;col_3=5".

Caveats
- For very large |Q|, consider replacing broadcast filtering with an inverted index (prototype left as TODO).
- For large |R| and high T, Method 2 can become scan-heavy (O(T×|R|)). Use smaller T or enable --sample_rate flag.
- For better caching and load balancing, run Spark in cluster mode.

Good luck!
