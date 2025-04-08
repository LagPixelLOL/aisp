import sys
import tqdm
import utils
import argparse
from constants import *
from collections import defaultdict

def parse_args():
    parser = argparse.ArgumentParser(description="Create model tags based on tag frequency.")
    parser.add_argument("-m", "--min-images", type=int, default=0, help="Filter out tags with less than the specified amount of images, default to 0")
    mutex = parser.add_mutually_exclusive_group()
    mutex.add_argument("-e", "--exclude", nargs="+", help="Exclude tag groups with the specified group names, you can only set either exclude or include, but not both")
    mutex.add_argument("-i", "--include", nargs="+", help="Include tag groups with the specified group names, you can only set either include or exclude, but not both")
    args = parser.parse_args()
    if args.min_images < 0:
        print("Minimum images must be greater than or equal to 0!")
        sys.exit(1)
    return args

def main():
    args = parse_args()
    print("Starting...\nGetting paths...")
    image_id_image_metadata_path_tuple_dict = utils.get_image_id_image_metadata_path_tuple_dict(IMAGE_DIR)
    print("Got", len(image_id_image_metadata_path_tuple_dict), "images.\nMaking buckets...")
    buckets = defaultdict(int)
    for _, metadata_path in tqdm.tqdm(image_id_image_metadata_path_tuple_dict.values(), desc="Making buckets"):
        for tag in utils.get_tags(metadata_path, args.exclude, args.include):
            buckets[tag] += 1
    ratings = []
    for bucket in list(buckets.items()):
        tag = bucket[0]
        if not tag.startswith("rating:"):
            continue
        ratings.append(bucket)
        buckets.pop(tag)
    print("Sorting the tags based on alphabetical order...")
    buckets = sorted(buckets.items())
    print("Filtering out tags with less than", args.min_images, "images...")
    buckets += ratings
    tags = [bucket[0] for bucket in buckets if bucket[1] >= args.min_images]
    print("The new model tags list contains", len(tags), "tags.\nSaving the result...")
    with open(MODEL_TAGS_PATH, "w", encoding="utf8") as file:
        for i, tag in enumerate(tags):
            file.write(f"{i} {tag}\n")
    print("Finished.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nScript interrupted by user, exiting...")
        sys.exit(1)
