import os
import re
import sys
import time
import json
import utils
import asyncio
import argparse
import concurrent
from constants import *
from bs4 import BeautifulSoup

IMAGE_ID_PATTERN = re.compile(r"id=(\d+)")

def get_type_tags_dict(soup):
    tag_ul = soup.find("ul", id="tag-list")
    if not tag_ul:
        raise RuntimeError("No tag list found in this web page!")
    type_tags_dict = {}
    tags_in_dict = set()
    for element in tag_ul.find_all("li"):
        class_name = element.get("class")
        if not class_name or len(class_name) != 1:
            continue
        class_name = class_name[0]
        if not class_name.startswith("tag-type-"):
            continue
        tag = element.find("a", recursive=False).contents[0].replace(",", "").replace(" ", "_").strip("_")
        if tag in tags_in_dict:
            continue
        tag_type = class_name[9:]
        tag_list = type_tags_dict.get(tag_type)
        if tag_list is None:
            type_tags_dict[tag_type] = [tag]
        else:
            tag_list.append(tag)
        tags_in_dict.add(tag)
    return type_tags_dict, len(tags_in_dict)

async def process_link(scrape_args, scrape_state):
    image_id = IMAGE_ID_PATTERN.search(scrape_args.target).group(1)
    scrape_state.last_reached_image_id = image_id
    image_id_already_exists = image_id in scrape_state.existing_image_ids
    if image_id_already_exists and not image_id.endswith("99"):
        # print(f"Image {image_id} already exists, skipped.")
        return
    scrape_state.existing_image_ids.add(image_id)
    error = None
    for i in range(1, MAX_RETRY + 2): # 1 indexed.
        try:
            if utils.get_sigint_count() >= 1 or isinstance(scrape_args.max_scrape_count, int) and scrape_state.scraped_image_count >= scrape_args.max_scrape_count:
                break
            # print(f"Processing image {image_id}...")
            query_start_time = time.time()
            async with scrape_state.session.get(scrape_args.target) as response:
                html = await response.text()
            query_used_time = time.time() - query_start_time
            soup = BeautifulSoup(html, "html.parser")

            video_container = soup.find("video", id="gelcomVideoPlayer")
            if video_container:
                print(f"Image {image_id} is a video, skipped.")
                return
            image_container = soup.find("section", class_=["image-container", "note-container"])
            if not image_container:
                raise RuntimeError("No image container found.")

            score_span = soup.find("span", id="psc" + image_id)
            try:
                image_score = int(score_span.contents[0])
            except (AttributeError, IndexError, ValueError) as e:
                raise RuntimeError("Error while getting the image score: " + str(e)) from e
            scrape_state.last_reached_image_score = image_score
            if image_id_already_exists:
                # print(f"Image {image_id} already exists, skipped.")
                return

            if not scrape_args.use_low_quality:
                image_download_url = soup.find("a", string="Original image")["href"]
            else:
                image_download_url = image_container.find("img", id="image")["src"]

            image_ext = os.path.splitext(image_download_url)[1].lower()
            if image_ext not in IMAGE_EXT:
                print(f"Image {image_id} is not an image, skipped.")
                return

            type_tags_dict, tag_count = get_type_tags_dict(soup)
            if tag_count < scrape_args.min_tags:
                # print(f"Image {image_id} doesn't have enough tags({tag_count} < {scrape_args.min_tags}), skipped.")
                return

            rating = image_container.get("data-rating")
            if not rating:
                raise RuntimeError("No rating found.")
            if rating == "safe":
                rating = "general"

            metadata = json.dumps({"image_id": image_id, "score": image_score, "rating": rating, "tags": type_tags_dict}, ensure_ascii=False, separators=(",", ":"))

            image_path = os.path.join(IMAGE_DIR, image_id + image_ext)
            metadata_path = os.path.join(IMAGE_DIR, image_id + ".json")

            download_start_time = time.time()
            async with scrape_state.session.get(image_download_url) as img_response:
                img_data = await img_response.read()
            download_used_time = time.time() - download_start_time

            if not await utils.submit_validation(scrape_state.thread_pool, img_data, metadata, image_path, metadata_path, scrape_args.width, scrape_args.height, scrape_args.convert_to_avif):
                return
            scrape_state.scraped_image_count += 1
            total_query_time = scrape_state.avg_query_time[0] * scrape_state.avg_query_time[1] + query_used_time
            total_download_time = scrape_state.avg_download_time[0] * scrape_state.avg_download_time[1] + download_used_time
            scrape_state.avg_query_time[1] += 1
            scrape_state.avg_download_time[1] += 1
            scrape_state.avg_query_time[0] = total_query_time / scrape_state.avg_query_time[1]
            scrape_state.avg_download_time[0] = total_download_time / scrape_state.avg_download_time[1]
            interval = 1000
            if scrape_state.scraped_image_count % interval != 0:
                return
            print(
                f"Scraped {scrape_state.scraped_image_count}/{scrape_args.max_scrape_count} images,",
                f"stats for the last {interval} images: [Average query time: {scrape_state.avg_query_time[0]:.3f}s | Average download time: {scrape_state.avg_download_time[0]:.3f}s]",
            )
            scrape_state.avg_query_time = [0.0, 0]
            scrape_state.avg_download_time = [0.0, 0]
            return
        except Exception as e:
            error = e
            if i > MAX_RETRY:
                break
            # print(f"A {e.__class__.__name__} occurred with image {image_id}: {e}\nPausing for 0.1 second before retrying attempt {i}/{MAX_RETRY}...")
            await asyncio.sleep(0.1)
    if not image_id_already_exists:
        scrape_state.existing_image_ids.remove(image_id)
    if error is not None:
        print(f"All retry attempts failed, image {image_id} skipped. Final error {error.__class__.__name__}: {error}")
    else:
        print(f"Task for image {image_id} cancelled.")

def parse_args():
    parser = argparse.ArgumentParser(description="Scrape images from Gelbooru.")
    parser.add_argument("-s", "--site", default="https://gelbooru.com", help="Domain to scrape from, default to https://gelbooru.com")
    parser.add_argument("-W", "--width", type=int, help="Scale the width of the image to the specified value, must either provide both width and height or not provide both")
    parser.add_argument("-H", "--height", type=int, help="Scale the height of the image to the specified value, must either provide both width and height or not provide both")
    parser.add_argument("-a", "--avif", action="store_true", help="If set, will convert the image into avif, need to have pillow-avif-plugin installed")
    parser.add_argument("-l", "--low-quality", action="store_true", help="If set, will download the sample instead of the original image")
    parser.add_argument("-t", "--min-tags", type=int, default=0, help="Filter out images with less than the specified amount of tags, default to 0")
    parser.add_argument("-m", "--max-scrape-count", type=int, help="Stop after scraping the set amount of images, may not be exact because of the asynchronous nature of this script, default to infinite")
    parser.add_argument("-c", "--continuous-scraping", action="store_true", help="If set, will scraping continuously even when reaching the 20000 images Gelbooru search depth cap by adjusting search tags")
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
    page_number = 0
    search_tags = utils.SearchTags(args.tags_to_search)

    os.makedirs(IMAGE_DIR, exist_ok=True)
    existing_image_ids = utils.get_existing_image_id_set(IMAGE_DIR)
    utils.register_sigint_callback()

    session_args = [TIMEOUT, {"fringeBenefits": "yup"}]
    scrape_state = utils.ScrapeState(concurrent.futures.ThreadPoolExecutor(max_workers=os.cpu_count()), utils.get_session(*session_args), existing_image_ids)
    session_refresh_counter = 0
    tasks = []
    while True:
        try:
            if utils.get_sigint_count() >= 1 or isinstance(args.max_scrape_count, int) and scrape_state.scraped_image_count >= args.max_scrape_count:
                break
            request_url = f"{args.site}/index.php?page=post&s=list&tags={search_tags.to_search_string()}&pid={page_number}"
            print(f"Going to {request_url}")
            async with scrape_state.session.get(request_url) as response:
                html = await response.text()
            soup = BeautifulSoup(html, "html.parser")
            thumbnails_div = soup.find("div", class_="thumbnail-container")
            if not thumbnails_div:
                raise RuntimeError("Thumbnails division not found.")
            notice_error = thumbnails_div.find("div", class_="notice error")
            if notice_error and args.continuous_scraping:
                print("Reached restricted depth, adjusting search tags to continue scraping...")
                search_tags.update_bound(scrape_state)
                page_number = 0
                continue
            image_urls = [a["href"] for a in thumbnails_div.find_all("a")]
            image_url_count = len(image_urls)
            if image_url_count == 0:
                print("Website returned 0 image urls.")
                break
            print(f"Got {image_url_count} posts.")
            page_number += image_url_count
            for image_url in image_urls:
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
                tasks.append(asyncio.create_task(process_link(utils.ScrapeArgs(image_url, args.width, args.height, args.avif, args.low_quality, args.min_tags, args.max_scrape_count), scrape_state)))
            if utils.get_sigint_count() >= 1 or isinstance(args.max_scrape_count, int) and scrape_state.scraped_image_count >= args.max_scrape_count:
                break
            session_refresh_counter += 1
            if session_refresh_counter % 50 == 0:
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
                    scrape_state.session = utils.get_session(*session_args)
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
