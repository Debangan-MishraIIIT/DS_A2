import os
import textwrap

global_chunk_counter = 0

def split_file_into_chunks(file_path, chunk_size=4096, output_dir="file_chunks"):
    global global_chunk_counter
    os.makedirs(output_dir, exist_ok=True)

    filename, ext = os.path.splitext(os.path.basename(file_path))  # Extract name without extension

    with open(file_path, "r", encoding="utf-8") as f:
        data = f.read()

    words = data.split()
    chunks = []
    current_chunk = []
    current_size = 0

    for word in words:
        word_size = len(word) + 1  # +1 for the space
        if current_size + word_size > chunk_size:
            chunks.append(" ".join(current_chunk))
            current_chunk = []
            current_size = 0
        current_chunk.append(word)
        current_size += word_size

    if current_chunk:
        chunks.append(" ".join(current_chunk))

    for chunk in chunks:
        chunk_filename = os.path.join(output_dir, f"{filename}_mapper_{global_chunk_counter}.txt")  # Correct format
        with open(chunk_filename, "w", encoding="utf-8") as chunk_file:
            chunk_file.write(chunk)
        global_chunk_counter += 1

def generate_test_files(directory="test_files", file_size=2000, num_files=2):
    os.makedirs(directory, exist_ok=True)
    sample_text = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. " * 100

    for i in range(num_files):
        file_path = os.path.join(directory, f"test_file_{i}.txt")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(sample_text[:file_size])

    return [os.path.join(directory, f"test_file_{i}.txt") for i in range(num_files)]

if __name__ == "__main__":
    test_files = generate_test_files()
    for test_file in test_files:
        split_file_into_chunks(test_file)
