import os
import shutil

# Define directories
reducer_dir = "reducer_intermediate"
mapper_dir = "mapper_intermediate"
chunks_dir = "file_chunks"
output_file = "output.txt"

# List to store (word, content) pairs
word_content_pairs = []

# Read and collect word-content pairs from reducer_intermediate
if os.path.exists(reducer_dir):
    for filename in sorted(os.listdir(reducer_dir)):  # Ensure consistent order
        file_path = os.path.join(reducer_dir, filename)
        if os.path.isfile(file_path) and filename.endswith(".txt"):
            with open(file_path, "r", encoding="utf-8") as infile:
                for line in infile:
                    parts = line.strip().split(" ", 1)  # Split only on first space
                    if len(parts) == 2:
                        word_content_pairs.append((parts[0], parts[1]))

# Sort alphabetically by word
word_content_pairs.sort()

# Write sorted output to output.txt
with open(output_file, "w", encoding="utf-8") as outfile:
    for word, content in word_content_pairs:
        outfile.write(f"{word} {content}\n")

# Remove intermediate directories
for directory in [reducer_dir, mapper_dir, chunks_dir]:
    if os.path.exists(directory):
        shutil.rmtree(directory)

print(f"Generated {output_file} with sorted word-content pairs and deleted intermediate directories.")
