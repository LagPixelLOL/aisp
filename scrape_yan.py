import os
import sys
import time
import json
import utils
import urllib
import asyncio
import argparse
import concurrent
from constants import *

TIMEOUT = 30 # Local override.

def get_type_tags_dict(raw_tags_text, tag_type_dict):
    type_tags_dict = {}
    tags_in_dict = set()
    for tag in raw_tags_text.split():
        tag = tag.replace(",", "").strip("_")
        if tag in tags_in_dict:
            continue
        tag_type = tag_type_dict.get(tag)
        if tag_type is None:
            raise ValueError(f"No tag type found for tag \"{tag}\"!")
        tag_list = type_tags_dict.get(tag_type)
        if tag_list is None:
            type_tags_dict[tag_type] = [tag]
        else:
            tag_list.append(tag)
        tags_in_dict.add(tag)
    return type_tags_dict, len(tags_in_dict)

async def process_image_object(scrape_args, scrape_state):
    image_id = str(scrape_args.target["id"])
    if image_id in scrape_state.existing_image_ids:
        # print(f"Image {image_id} already exists, skipped.")
        return
    scrape_state.existing_image_ids.add(image_id)
    error = None
    for i in range(1, MAX_RETRY + 2): # 1 indexed.
        try:
            if utils.get_sigint_count() >= 1 or isinstance(scrape_args.max_scrape_count, int) and scrape_state.scraped_image_count >= scrape_args.max_scrape_count:
                break
            # print(f"Processing image {image_id}...")
            if not scrape_args.use_low_quality:
                image_download_url = scrape_args.target["file_url"]
            else:
                image_download_url = scrape_args.target["sample_url"]

            image_ext = os.path.splitext(image_download_url)[1].lower()
            if image_ext not in IMAGE_EXT:
                print(f"Image {image_id} is not an image, skipped.")
                return

            type_tags_dict, tag_count = get_type_tags_dict(scrape_args.target["tags"], scrape_args.tag_type_dict)
            if tag_count < scrape_args.min_tags:
                # print(f"Image {image_id} doesn't have enough tags({tag_count} < {scrape_args.min_tags}), skipped.")
                return

            rating = scrape_args.target.get("rating")
            match rating:
                case "s":
                    rating = "general"
                case "q":
                    rating = "questionable"
                case "e":
                    rating = "explicit"
                case _:
                    raise RuntimeError(f"Unknown rating: {rating}")

            metadata = json.dumps({"image_id": image_id, "score": scrape_args.target["score"], "rating": rating, "tags": type_tags_dict}, ensure_ascii=False, separators=(",", ":"))

            image_path = os.path.join(IMAGE_DIR, image_id + image_ext)
            metadata_path = os.path.join(IMAGE_DIR, image_id + ".json")

            download_start_time = time.time()
            async with scrape_state.session.get(image_download_url) as img_response:
                img_data = await img_response.read()
            download_used_time = time.time() - download_start_time

            if not await utils.submit_validation(scrape_state.thread_pool, img_data, metadata, image_path, metadata_path, scrape_args.width, scrape_args.height, scrape_args.convert_to_avif):
                return
            scrape_state.scraped_image_count += 1
            total_download_time = scrape_state.avg_download_time[0] * scrape_state.avg_download_time[1] + download_used_time
            scrape_state.avg_download_time[1] += 1
            scrape_state.avg_download_time[0] = total_download_time / scrape_state.avg_download_time[1]
            interval = 1000
            if scrape_state.scraped_image_count % interval != 0:
                return
            print(
                f"Scraped {scrape_state.scraped_image_count}/{scrape_args.max_scrape_count} images,",
                f"stats for the last {interval} images: [Average download time: {scrape_state.avg_download_time[0]:.3f}s]",
            )
            scrape_state.avg_download_time = [0.0, 0]
            return
        except Exception as e:
            error = e
            if i > MAX_RETRY:
                break
            # print(f"A {e.__class__.__name__} occurred with image {image_id}: {e}\nPausing for 0.1 second before retrying attempt {i}/{MAX_RETRY}...")
            await asyncio.sleep(0.1)
    scrape_state.existing_image_ids.remove(image_id)
    if error is not None:
        print(f"All retry attempts failed, image {image_id} skipped. Final error {error.__class__.__name__}: {error}")
    else:
        print(f"Task for image {image_id} cancelled.")

def parse_args():
    parser = argparse.ArgumentParser(description="Scrape images from yande.re.")
    parser.add_argument("-s", "--site", default="https://yande.re", help="Domain to scrape from, default to https://yande.re")
    parser.add_argument("-W", "--width", type=int, help="Scale the width of the image to the specified value, must either provide both width and height or not provide both")
    parser.add_argument("-H", "--height", type=int, help="Scale the height of the image to the specified value, must either provide both width and height or not provide both")
    parser.add_argument("-a", "--avif", action="store_true", help="If set, will convert the image into avif, need to have pillow-avif-plugin installed")
    parser.add_argument("-l", "--low-quality", action="store_true", help="If set, will download the sample instead of the original image")
    parser.add_argument("-t", "--min-tags", type=int, default=0, help="Filter out images with less than the specified amount of tags, default to 0")
    parser.add_argument("-m", "--max-scrape-count", type=int, help="Stop after scraping the set amount of images, may not be exact because of the asynchronous nature of this script, default to infinite")
    parser.add_argument("tags_to_search", nargs=argparse.REMAINDER, help="List of tags to search for, default to all")
    args = parser.parse_args()
    if args.width is None or args.height is None:
        if args.width is not None or args.height is not None:
            print("You must either provide both width and height or not provide both at the same time!")
            sys.exit(1)
    else:
        if args.width < 1:
            print("Width must be greater than or equal to 1!")
            sys.exit(1)
        if args.height < 1:
            print("Height must be greater than or equal to 1!")
            sys.exit(1)
    if args.avif:
        try:
            import pillow_avif
        except ImportError:
            print("You need to pip install pillow-avif-plugin to use avif conversion!")
            sys.exit(1)
    if args.min_tags < 0:
        print("Minimum tags must be greater than or equal to 0!")
        sys.exit(1)
    if isinstance(args.max_scrape_count, int) and args.max_scrape_count <= 0:
        print("Maximum scrape count must be greater than 0!")
        sys.exit(1)
    return args

async def main():
    args = parse_args()
    print("Starting...")
    page_number = 1
    search_tags = "+".join(urllib.parse.quote(tag, safe="") for tag in args.tags_to_search)

    os.makedirs(IMAGE_DIR, exist_ok=True)
    existing_image_ids = utils.get_existing_image_id_set(IMAGE_DIR)
    utils.register_sigint_callback()

    scrape_state = utils.ScrapeState(concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()), utils.get_session(TIMEOUT), existing_image_ids)
    tasks = []
    while True:
        try:
            if utils.get_sigint_count() >= 1 or isinstance(args.max_scrape_count, int) and scrape_state.scraped_image_count >= args.max_scrape_count:
                break
            request_url = f"{args.site}/post.json?api_version=2&include_tags=1&limit=1000&tags={search_tags}&page={page_number}"
            print(f"Going to {request_url}")
            async with scrape_state.session.get(request_url) as response:
                response_json = await response.json()
            image_objects = response_json["posts"]
            image_count = len(image_objects)
            if image_count == 0:
                print("Website returned 0 images.")
                break
            print(f"Got {image_count} posts.")
            tag_type_dict = {tag.replace(",", "").strip("_"): type for tag, type in response_json["tags"].items()}
            page_number += 1
            for image_object in image_objects:
                if utils.get_sigint_count() >= 1 or isinstance(args.max_scrape_count, int) and scrape_state.scraped_image_count >= args.max_scrape_count:
                    break
                while len(tasks) >= MAX_TASKS:
                    if utils.get_sigint_count() >= 1 or isinstance(args.max_scrape_count, int) and scrape_state.scraped_image_count >= args.max_scrape_count:
                        break
                    await asyncio.sleep(0.1)
                    for i in range(len(tasks) - 1, -1, -1):
                        task = tasks[i]
                        if task.done():
                            await task
                            del tasks[i]
                tasks.append(asyncio.create_task(process_image_object(
                    utils.ScrapeArgs(image_object, args.width, args.height, args.avif, args.low_quality, args.min_tags, args.max_scrape_count, tag_type_dict), scrape_state
                )))
            if utils.get_sigint_count() >= 1 or isinstance(args.max_scrape_count, int) and scrape_state.scraped_image_count >= args.max_scrape_count:
                break
            if page_number % 2 == 1:
                print("Refreshing session...")
                while tasks and utils.get_sigint_count() < 1:
                    await asyncio.sleep(0.1)
                    for i in range(len(tasks) - 1, -1, -1):
                        task = tasks[i]
                        if task.done():
                            await task
                            del tasks[i]
                if utils.get_sigint_count() < 1:
                    await scrape_state.session.close()
                    scrape_state.session = utils.get_session(TIMEOUT)
        except Exception as e:
            print(f"An error occurred: {e}\nPausing for 0.1 second before retrying...")
            await asyncio.sleep(0.1)
    if utils.get_sigint_count() >= 1:
        print("Script interrupted by user, gracefully exiting...\nYou can interrupt again to exit semi-forcefully, but it will break image checks!")
    else:
        print("No more images to download, waiting already submitted tasks to finish...")
    while tasks and utils.get_sigint_count() <= 1:
        await asyncio.sleep(0.1)
        for i in range(len(tasks) - 1, -1, -1):
            task = tasks[i]
            if task.done():
                await task
                del tasks[i]
    await scrape_state.session.close()
    if utils.get_sigint_count() >= 1:
        if utils.get_sigint_count() >= 2:
            print("Another interrupt received, exiting semi-forcefully...\nYou can interrupt again for truly forceful exit, but it most likely will break a lot of things!")
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nScript interrupted by user, exiting...")
        sys.exit(1)
