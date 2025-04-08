import os
import io
import copy
import json
import asyncio
import aiohttp
from PIL import Image

def validate_image(image_data, metadata, image_path, metadata_path, width=None, height=None, convert_to_avif=False):
    try:
        with io.BytesIO(image_data) as image_filelike:
            with Image.open(image_filelike) as img:
                save_kwargs = {}
                if isinstance(width, int) and width > 0 and isinstance(height, int) and height > 0:
                    img = img.resize((width, height))
                if convert_to_avif:
                    import pillow_avif
                    save_kwargs["quality"] = 50
                    image_path = os.path.splitext(image_path)[0] + ".avif"
                img.load()
                img.save(image_path, **save_kwargs)
        with open(metadata_path, "w", encoding="utf8") as metadata_file:
            metadata_file.write(metadata)
        return True
    except Exception as e:
        print(f"Error validating image {image_path}: {e}")
        try:
            os.remove(image_path)
        except FileNotFoundError:
            pass
        except Exception as e:
            print("Error deleting image file:", e)
        try:
            os.remove(metadata_path)
            print(f"Deleted invalid image and metadata files: \"{image_path}\", \"{metadata_path}\"")
        except FileNotFoundError:
            pass
        except Exception as e:
            print("Error deleting metadata file:", e)
    return False

async def submit_validation(thread_pool, image_data, metadata, image_path, metadata_path, width=None, height=None, convert_to_avif=False):
    return await asyncio.wrap_future(thread_pool.submit(validate_image, image_data, metadata, image_path, metadata_path, width, height, convert_to_avif))

def get_image_id_image_metadata_path_tuple_dict(image_dir):
    if not os.path.isdir(image_dir):
        raise FileNotFoundError(f"\"{image_dir}\" is not a directory!")
    image_id_image_metadata_path_tuple_dict = {}
    for path in os.listdir(image_dir):
        image_id, ext = os.path.splitext(path)
        if ext == ".json":
            continue
        path = os.path.join(image_dir, path)
        if not os.path.isfile(path):
            continue
        metadata_path = os.path.splitext(path)[0] + ".json"
        if not os.path.isfile(metadata_path):
            continue
        image_id_image_metadata_path_tuple_dict[image_id] = (path, metadata_path)
    return image_id_image_metadata_path_tuple_dict

def get_existing_image_id_set(image_dir):
    return set(get_image_id_image_metadata_path_tuple_dict(image_dir))

def get_session(timeout=None, cookies=None):
    kwargs = {"connector": aiohttp.TCPConnector(limit=0, ttl_dns_cache=600), "cookies": cookies}
    if timeout is not None:
        kwargs["timeout"] = aiohttp.ClientTimeout(total=timeout)
    return aiohttp.ClientSession(**kwargs)

def get_model_tags(model_tags_path):
    if not os.path.isfile(model_tags_path):
        raise FileNotFoundError(f"\"{model_tags_path}\" is not a file, please place one there!")
    index_tag_dict = {}
    with open(model_tags_path, "r", encoding="utf8") as model_tags_file:
        for line in model_tags_file:
            line = line.split()
            if len(line) != 2:
                continue
            index_tag_dict[int(line[0])] = line[1]
    if len(index_tag_dict) <= 0:
        return []
    sorted_index_tag_tuple_list = sorted(index_tag_dict.items(), key=lambda x: x[0])
    if len(sorted_index_tag_tuple_list) != sorted_index_tag_tuple_list[-1][0] + 1:
        raise ValueError(f"The index specified in \"{model_tags_path}\" is not continuous!")
    return [tag for _, tag in sorted_index_tag_tuple_list]

def get_metadata(metadata_path):
    if not os.path.isfile(metadata_path):
        raise FileNotFoundError(f"\"{metadata_path}\" is not a file!")
    with open(metadata_path, "r", encoding="utf8") as metadata_file:
        return json.load(metadata_file)

def get_tags(metadata_path_or_dict, exclude=None, include=None, no_rating_prefix=False):
    if exclude is not None and include is not None:
        raise ValueError("You can't set both exclude and include, please only set one.")
    metadata = get_metadata(metadata_path_or_dict) if not isinstance(metadata_path_or_dict, dict) else metadata_path_or_dict
    type_tags_dict = copy.copy(metadata.get("tags", {}))
    if exclude is not None:
        for e in exclude:
            type_tags_dict.pop(e, None)
        include_rating = "rating" not in exclude
    elif include is not None:
        for k in list(type_tags_dict):
            if k not in include:
                type_tags_dict.pop(k)
        include_rating = "rating" in include
    else:
        include_rating = True
    tags = []
    for l in type_tags_dict.values():
        tags += l
    if include_rating:
        tags.append(("" if no_rating_prefix else "rating:") + metadata["rating"])
    return tags
