import os
import sys
import tqdm
import utils
import shutil
import random
import argparse
from constants import *

def parse_args():
    parser = argparse.ArgumentParser(description="Balance the dataset based on tag frequency.")
    parser.add_argument("-c", "--count", type=int, help="The target selection count, must be an integer greater than 0")
    parser.add_argument("-d", "--display", action="store_true", help="Display the count of images in each bucket")
    parser.add_argument("-r", "--reverse", action="store_true", help="Display in reverse order, only for displaying")
    args = parser.parse_args()
    if not args.display:
        if args.reverse:
            print("You can't specify reverse when not using display mode!")
            sys.exit(1)
        if not isinstance(args.count, int):
            print("You must specify the target selection count when not using display mode!")
            sys.exit(1)
        if args.count <= 0:
            print("Target selection count must be an integer greater than 0!")
            sys.exit(1)
    elif isinstance(args.count, int):
        print("You can't specify the target selection count when using display mode!")
        sys.exit(1)
    return args

def main():
    args = parse_args()
    print("Starting...\nGetting model tags...")
    model_tags = utils.get_model_tags(MODEL_TAGS_PATH)
    print("Getting paths...")
    image_id_image_metadata_path_tuple_tuple_list = sorted(utils.get_image_id_image_metadata_path_tuple_dict(IMAGE_DIR).items(), key=lambda x: x[0])
    print("Got", len(image_id_image_metadata_path_tuple_tuple_list), "images.\nShuffling paths...")
    random.seed(42)
    random.shuffle(image_id_image_metadata_path_tuple_tuple_list)
    print("Making buckets...")
    in_bucket_image_count = 0
    buckets = {tag: [] for tag in model_tags}
    for image_id_image_metadata_path_tuple_tuple in tqdm.tqdm(image_id_image_metadata_path_tuple_tuple_list, desc="Making buckets"):
        did_append = False
        for tag in utils.get_tags(image_id_image_metadata_path_tuple_tuple[1][1]):
            bucket = buckets.get(tag)
            if bucket is None:
                continue
            bucket.append(image_id_image_metadata_path_tuple_tuple)
            did_append = True
        if did_append:
            in_bucket_image_count += 1
    print("Got", in_bucket_image_count, "unique images in buckets.")
    buckets = sorted(buckets.items(), key=lambda x: len(x[1]))
    if args.display:
        if args.reverse: range_iter = range(len(buckets) - 1, -1, -1)
        else: range_iter = range(len(buckets))
        for i in range_iter: print(buckets[i][0], len(buckets[i][1]))
        return
    print("Selecting...")
    total = min(args.count, in_bucket_image_count)
    selected = {} # Key: Image ID, Value: (Image path, Metadata path).
    with tqdm.tqdm(total=total, desc="Selecting") as progress_bar:
        while len(selected) < total:
            for tag, image_id_image_metadata_path_tuple_tuple_list in buckets:
                if len(selected) >= total:
                    break
                if len(image_id_image_metadata_path_tuple_tuple_list) <= 0:
                    continue
                for i in range(len(image_id_image_metadata_path_tuple_tuple_list) - 1, -1, -1):
                    if image_id_image_metadata_path_tuple_tuple_list[i][0] in selected:
                        del image_id_image_metadata_path_tuple_tuple_list[i]
                        break
                else:
                    last_item = image_id_image_metadata_path_tuple_tuple_list[-1]
                    selected[last_item[0]] = last_item[1]
                    del image_id_image_metadata_path_tuple_tuple_list[-1]
                    progress_bar.update(1)
    print("Selected", len(selected), "images.\nDeleting unselected images...")
    temp_dir = "__tag_bal_trans_tmp__"
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)
    for image_metadata_path_tuple in tqdm.tqdm(selected.values(), desc="Moving"):
        image_path = image_metadata_path_tuple[0]
        metadata_path = image_metadata_path_tuple[1]
        os.rename(image_path, os.path.join(temp_dir, os.path.basename(image_path)))
        os.rename(metadata_path, os.path.join(temp_dir, os.path.basename(metadata_path)))
    shutil.rmtree(IMAGE_DIR)
    shutil.move(temp_dir, IMAGE_DIR)
    print("Finished.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nScript interrupted by user, exiting...")
        sys.exit(1)
