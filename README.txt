============================================================
Finding the most Valuable parts of a Dataset — Project Skeleton
============================================================
Structure (IMPORTANT: names follow the instructor's script):
- Src/         : all source code + README + requirements
- Data/        : datasets and outputs (results/)
- Report/      : put XX.pdf here (final report)
- Presentation/: put XX.pptx here (final slides)
- Data/results/: outputs of the methods

Quick Start (local Spark):
1) Create synthetic table and queries (adjust sizes as needed):
   spark-submit Src/generate_dataset.py --rows 20000 --cols 6 --out Data/synthetic_R.csv
   spark-submit Src/generate_queries.py --num_queries 500 --cols 6 --tightness 2 --in Data/synthetic_R.csv --out Data/synthetic_Q.csv

2) Run Method 1 (popularity):
   Python3 main.py --method 1 --input Data/synthetic_R.csv --queries Data/synthetic_Q.csv --threshold 500 --output Data/results/method1_output.csv

3) Run Method 2 (pop × diversity, centroid-approx greedy):
   python3 main.py --method 2 \
  --input ../Data/synthetic_R.csv \
  --queries ../Data/synthetic_Q.csv \
  --threshold 500 \
  --output ../Data/results/method2_output.csv \
  --distance euclidean \
  --standardize


4) (Optional) Evaluate multiple sizes (toy example):
   spark-submit Src/evaluate.py --base_rows 20000 --scales 1,2,4 --cols 6 --num_queries 500 --threshold 500

Notes
- Method 1 is an exact "top-T by popularity".
- Method 2 uses a practical greedy that approximates average pairwise difference by distance to the current centroid.
  It updates the centroid incrementally and recomputes candidate gains each round (parallelizable in Spark).
- All files are CSV; the first line of R contains headers: col_0,...,col_{C-1}
- Query file syntax: CSV with columns [q_id, conds]; conds is semicolon-separated clauses like "col_0=12;col_3=5".

Caveats
- For very large |Q|, switch from broadcast evaluation to inverted index (left as an advanced TODO in method1/method2).
- For very large |R| and large T, method2 will be scan-heavy (O(T * |R|)); reduce T or enable sampling (--sample_rate).
- This skeleton is meant to be *runnable and modifiable*. You can swap distance functions or add proper inverted indices.

Good luck!
