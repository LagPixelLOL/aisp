import sys
import json
import tqdm
import utils
import base64
import aiohttp
import asyncio
import aiofiles
import argparse
import mimetypes
from constants import *

FEW_SHOT_EXAMPLES_PATH = "nl_llm_tag_few_shot_examples"
MAX_TOKENS = 2048
TEMPERATURE = 0.1
TOP_P = 0.9

def process_tags(tags):
    if not tags:
        return "unknown"
    return ", ".join(tag.replace("_", " ") for tag in tags)

async def get_user_prompt(metadata, image_path):
    artist_tags_text = process_tags(utils.get_tags(metadata, include="artist"))
    character_tags_text = process_tags(utils.get_tags(metadata, include="character"))
    copyright_tags_text = process_tags(utils.get_tags(metadata, include="copyright"))
    general_tags_text = process_tags(utils.get_tags(metadata, include="general"))
    rating_tag_text = utils.get_tags(metadata, include="rating", no_rating_prefix=True)[0]

    mime_type, _ = mimetypes.guess_type(image_path)
    async with aiofiles.open(image_path, "rb") as image_file:
        image_b64 = base64.b64encode(await image_file.read()).decode("utf8")

    return {"role": "user", "content": [
        {"type": "image_url", "image_url": {"url": f"data:{mime_type};base64,{image_b64}"}},
        {"type": "text", "text": f"""Tag context for the above image:
\"\"\"
Artist(s): {artist_tags_text}
Character(s): {character_tags_text}
Copyright source(s): {copyright_tags_text}
Tags: {general_tags_text}
NSFW Rating: {rating_tag_text}
\"\"\""""},
    ]}

async def nl_llm_tag(few_shot_examples, image_metadata_path_tuple, session, api_url, model_name):
    metadata = utils.get_metadata(image_metadata_path_tuple[1])
    for i in range(1, MAX_RETRY + 2): # 1 indexed.
        try:
            async with session.post(api_url, json={
                "model": model_name, "max_tokens": MAX_TOKENS, "temperature": TEMPERATURE, "top_p": TOP_P,
                "messages": [
                    {"role": "system", "content": """Describe the given image for a request from the user using the provided tags as ground truth.
"unknown" tag means the name can't be found, so you shouldn't mention it. If there are conflict between your image view and the tags, adhere to the tags.
You should include the artist(s), character(s), copyright source(s), and NSFW rating which are specified along the image in your response, for names, you should capitalize first letter to follow grammar rules.
Don't say the image is anime, illustration, etc., just describe what it has. Because it's assumed that most images are anime, except when the tags specify it's from real life then you should mention it.
Never use woman or man, the tags always refer to them as girl or boy. If you need to describe a thing, use the words and wording styles from the tags unless necessary.
You are required to be direct and vulgar when describing NSFW features and actions if the image is NSFW. If the tags contain "sex", it means the character(s) are having sex, and you should mention it.
If there are multiple characters, when describing their positions, features or poses, never refer to them with their names, use relative positions like to the left of the image, above a certain object, etc..
You should describe their names separately in a different sentence. If the tags talk about character(s) being a furry, you should mention it.
Never start your response with "The image depicts...", imagine you are directly telling an image generator so that it generates the image above, but also don't start your response with "Generate an image...".
For example, lets say if an image has a cat girl doing some things, you should start with "A cat girl with (appearance) doing (things)...", but don't follow this exactly, be creative.
Your response should be long and detailed, containing background scene description, character position, pose, and more too if there's any, basically include everything the tags have told you.
Don't use new lines, put your entire response into a single line. Start the description immediately, don't add starter or ending extra texts."""},
                    *few_shot_examples,
                    await get_user_prompt(metadata, image_metadata_path_tuple[0]),
                ],
            }) as response:
                response.raise_for_status()
                j = await response.json()
            break
        except Exception as e:
            if i > MAX_RETRY:
                raise RuntimeError(f"All retry attempts failed for \"{image_metadata_path_tuple[0]}\"! Final error {e.__class__.__name__}: {e}") from e
            tqdm.tqdm.write(f"A {e.__class__.__name__} occurred for \"{image_metadata_path_tuple[0]}\": {e}\nPausing for 0.1 second before retrying attempt {i}/{MAX_RETRY}...")
            await asyncio.sleep(0.1)

    choice = j["choices"][0]
    # tqdm.tqdm.write(f"Request for image \"{image_metadata_path_tuple[0]}\" token usage (input -> output): {j["usage"]["prompt_tokens"]} -> {j["usage"]["completion_tokens"]}")
    if choice["finish_reason"] == "length":
        raise RuntimeError(f"Request for \"{image_metadata_path_tuple[0]}\" finished too early!")
    metadata["nl_desc"] = choice["message"]["content"]
    async with aiofiles.open(image_metadata_path_tuple[1], "w", encoding="utf8") as result_metadata_file:
        await result_metadata_file.write(json.dumps(metadata, ensure_ascii=False, separators=(",", ":")))

def parse_args():
    parser = argparse.ArgumentParser(description="Tag images with natural language using a LLM.")
    parser.add_argument("-a", "--api", default="http://127.0.0.1:12345/v1", help="OpenAI compatible API URL prefix, default to http://127.0.0.1:12345/v1")
    parser.add_argument("-m", "--model", default="gpt-4-1106-preview", help="Model name to use, default to gpt-4-1106-preview")
    parser.add_argument("-c", "--concurrency", type=int, default=MAX_TASKS, help=f"Max concurrent requests, default to {MAX_TASKS}")
    args = parser.parse_args()
    args.api += "/chat/completions"
    if args.concurrency < 1:
        print("Max concurrent requests must be at least 1!")
        sys.exit(1)
    return args

async def main():
    args = parse_args()
    print("Starting...\nGetting few shot examples...")
    try:
        few_shot_examples_dict = utils.get_image_id_image_metadata_path_tuple_dict(FEW_SHOT_EXAMPLES_PATH)
    except FileNotFoundError:
        few_shot_examples_dict = {}
    few_shot_examples = []
    for few_shot_image_path, few_shot_metadata_path in few_shot_examples_dict.values():
        few_shot_metadata = utils.get_metadata(few_shot_metadata_path)
        few_shot_examples.append(await get_user_prompt(few_shot_metadata, few_shot_image_path))
        few_shot_examples.append({"role": "assistant", "content": few_shot_metadata["nl_desc"]})
    print("Got", len(few_shot_examples_dict), "few shot examples.\nGetting paths...")
    image_id_image_metadata_path_tuple_dict = utils.get_image_id_image_metadata_path_tuple_dict(IMAGE_DIR)
    image_count = len(image_id_image_metadata_path_tuple_dict)
    print("Got", image_count, "images.")

    tasks = []
    async with utils.get_session(0) as session:
        with tqdm.tqdm(total=image_count, desc="Requesting") as pbar:
            for image_metadata_path_tuple in image_id_image_metadata_path_tuple_dict.values():
                while len(tasks) >= args.concurrency:
                    await asyncio.sleep(0.1)
                    for i in range(len(tasks) - 1, -1, -1):
                        task = tasks[i]
                        if task.done():
                            await task
                            del tasks[i]
                            pbar.update(1)
                tasks.append(asyncio.create_task(nl_llm_tag(few_shot_examples, image_metadata_path_tuple, session, args.api, args.model)))

            while tasks:
                await asyncio.sleep(0.1)
                for i in range(len(tasks) - 1, -1, -1):
                    task = tasks[i]
                    if task.done():
                        await task
                        del tasks[i]
                        pbar.update(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nScript interrupted by user, exiting...")
        sys.exit(1)
