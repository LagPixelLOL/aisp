import os
import sys
import argparse
import tarfile
from constants import *
import concurrent.futures

def decompress_chunk(chunk_file, output_dir):
    with tarfile.open(chunk_file, "r") as tar:
        tar.extractall(path=output_dir)

def parse_args():
    parser = argparse.ArgumentParser(description="Extract files from chunked tar archives.")
    parser.add_argument("-i", "--input-dir", default=COMPRESSED_DIR, help="Input directory containing tar chunks")
    parser.add_argument("-o", "--output-dir", default=IMAGE_DIR, help="Output directory for extracted files")
    args = parser.parse_args()
    return args

def main():
    args = parse_args()
    if not os.path.isdir(args.input_dir):
        print(f"Your input dir \"{args.input_dir}\" doesn't exist or isn't a directory!")
        sys.exit(1)
    chunk_files = [os.path.join(args.input_dir, f) for f in os.listdir(args.input_dir) if f.endswith(".tar")]
    os.makedirs(args.output_dir, exist_ok=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = []
        for chunk_file in chunk_files:
            future = executor.submit(decompress_chunk, chunk_file, args.output_dir)
            futures.append(future)
        concurrent.futures.wait(futures)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nScript interrupted by user, exiting...")
        sys.exit(1)
