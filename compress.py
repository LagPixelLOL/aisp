import os
import sys
import utils
import argparse
import tarfile
from constants import *
import concurrent.futures

def compress_chunk(chunk, chunk_index, output_dir):
    with tarfile.open(os.path.join(output_dir, f"chunk_{chunk_index}.tar"), "w") as tar:
        for image_path, metadata_path in chunk:
            tar.add(image_path, arcname=os.path.basename(image_path))
            tar.add(metadata_path, arcname=os.path.basename(metadata_path))

def parse_args():
    parser = argparse.ArgumentParser(description="Group images into uncompressed tar files.")
    parser.add_argument("-i", "--input-dir", default=IMAGE_DIR, help="Input directory for the images to chunk into tars")
    parser.add_argument("-o", "--output-dir", default=COMPRESSED_DIR, help="Output directory for chunked tars")
    parser.add_argument("-n", "--num-images-per-chunk", type=int, default=sys.maxsize, help="Number of images per chunk, default to infinite")
    args = parser.parse_args()
    if args.num_images_per_chunk < 1:
        print("Number of images per chunk needs to be a positive integer!")
        sys.exit(1)
    return args

def main():
    args = parse_args()
    image_metadata_path_tuple_list = [e[1] for e in sorted(utils.get_image_id_image_metadata_path_tuple_dict(args.input_dir).items(), key=lambda x: x[0])]
    os.makedirs(args.output_dir, exist_ok=True)
    with concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = []
        for i in range(0, len(image_metadata_path_tuple_list), args.num_images_per_chunk):
            chunk = image_metadata_path_tuple_list[i:i + args.num_images_per_chunk]
            chunk_index = i // args.num_images_per_chunk
            future = executor.submit(compress_chunk, chunk, chunk_index, args.output_dir)
            futures.append(future)
        concurrent.futures.wait(futures)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nScript interrupted by user, exiting...")
        sys.exit(1)
