import os
import sys
import tqdm
import utils
import random
import argparse
from constants import *

def parse_args():
    parser = argparse.ArgumentParser(description="Convert JSON image metadata to TXT image tags.")
    parser.add_argument("-n", "--no-delete", action="store_true", help="If set, won't delete the JSON image metadata files")
    mutex = parser.add_mutually_exclusive_group()
    mutex.add_argument("-e", "--exclude", nargs="+", help="Exclude tag groups with the specified group names, you can only set either exclude or include, but not both")
    mutex.add_argument("-i", "--include", nargs="+", help="Include tag groups with the specified group names, you can only set either include or exclude, but not both")
    parser.add_argument("-p", "--no-rating-prefix", action="store_true", help="If set, won't prepend the \"rating:\" prefix to the rating")
    return parser.parse_args()

def main():
    args = parse_args()
    print("Starting...\nGetting paths...")
    image_id_image_metadata_path_tuple_dict = utils.get_image_id_image_metadata_path_tuple_dict(IMAGE_DIR)
    print("Got", len(image_id_image_metadata_path_tuple_dict), "images.")
    for _, metadata_path in tqdm.tqdm(image_id_image_metadata_path_tuple_dict.values(), desc="Converting"):
        tags = utils.get_tags(metadata_path, args.exclude, args.include, args.no_rating_prefix)
        random.shuffle(tags)
        tags_text = ", ".join(tag.replace("_", " ") for tag in tags)
        with open(os.path.splitext(metadata_path)[0] + ".txt", "w", encoding="utf8") as tags_file:
            tags_file.write(tags_text)
        if not args.no_delete:
            os.remove(metadata_path)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nScript interrupted by user, exiting...")
        sys.exit(1)
