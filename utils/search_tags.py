import re
import urllib
from typing import Optional
from dataclasses import dataclass

COMPARE_FILTER_TAG_PATTERN = re.compile(r"^([a-z]+):([<>]?)(=?)(\S*)$", re.ASCII)
WHITE_SPACE_PATTERN = re.compile(r"\s")

@dataclass
class SortTag:
    sort_type: str = "id"
    descending: bool = True

    def __str__(self):
        return "sort:" + self.sort_type + ":" + ("desc" if self.descending else "asc")

    @classmethod
    def validate_sort_type(cls, sort_type):
        match sort_type:
            case "id":
                pass
            case "score":
                pass
            case _:
                raise NotImplementedError(f"Sort type \"{sort_type}\" is not implemented!")

    @classmethod
    def from_tag(cls, tag):
        if not tag.startswith("sort:"):
            return None
        sort_type = None
        descending = True
        for i, sort_tag_part in enumerate(tag.split(":")):
            match i:
                case 0:
                    pass
                case 1:
                    cls.validate_sort_type(sort_tag_part)
                    sort_type = sort_tag_part
                case 2:
                    if sort_tag_part == "asc":
                        descending = False
                case _:
                    raise ValueError(f"The sort tag \"{tag}\" you provided isn't valid!")
        if i < 1:
            raise ValueError(f"The sort tag \"{tag}\" you provided isn't valid!")
        return cls(sort_type, descending)

@dataclass
class CompareFilterTag:
    compare_type: str
    less_than: bool
    with_equal: bool
    target: str

    def __str__(self):
        return self.compare_type + ":" + ("<" if self.less_than else ">") + ("=" if self.with_equal else "") + self.target

    @classmethod
    def from_tag(cls, tag):
        re_match = COMPARE_FILTER_TAG_PATTERN.search(tag)
        if re_match is None:
            return None
        target = re_match.group(4)
        if not target:
            raise ValueError(f"The compare filter tag \"{tag}\" you provided isn't valid!")
        less_than = re_match.group(2)
        with_equal = re_match.group(3)
        if not less_than:
            if not with_equal:
                return None
            raise ValueError(f"The compare filter tag \"{tag}\" you provided isn't valid!")
        return cls(re_match.group(1), less_than == "<", bool(with_equal), target)

class SearchTags:

    def __init__(self, tags: list[str]):
        self.general_tags: list[str] = []
        self.sort_tag: SortTag = None
        self.compare_filter_tags: list[CompareFilterTag] = []
        self.sort_associated_compare_filter_tag: Optional[CompareFilterTag] = None
        for tag in tags:
            tag = tag.strip().lower()
            if not tag:
                continue
            if WHITE_SPACE_PATTERN.search(tag):
                raise ValueError(f"The tag \"{tag}\" contains white space(s), booru tags should use \"_\" instead of spaces!")
            sort_tag = SortTag.from_tag(tag)
            if sort_tag is not None:
                if self.sort_tag is not None:
                    raise ValueError("You can't provide more than 1 sort tag!")
                self.sort_tag = sort_tag
                continue
            compare_filter_tag = CompareFilterTag.from_tag(tag)
            if compare_filter_tag is not None:
                self.compare_filter_tags.append(compare_filter_tag)
                continue
            self.general_tags.append(tag)
        if self.sort_tag is None:
            self.sort_tag = SortTag()
        for i in range(len(self.compare_filter_tags) - 1, -1, -1):
            compare_filter_tag = self.compare_filter_tags[i]
            if compare_filter_tag.compare_type == self.sort_tag.sort_type and compare_filter_tag.less_than == self.sort_tag.descending:
                if self.sort_associated_compare_filter_tag is not None:
                    raise ValueError("You can't provide more than 1 sort associated compare filter tag!")
                self.sort_associated_compare_filter_tag = compare_filter_tag
                del self.compare_filter_tags[i]

    def update_bound(self, scrape_state):
        match self.sort_tag.sort_type:
            case "id":
                if scrape_state.last_reached_image_id is None:
                    raise ValueError("Last reached image ID isn't set!")
                self.sort_associated_compare_filter_tag = CompareFilterTag("id", self.sort_tag.descending, True, scrape_state.last_reached_image_id)
            case "score":
                if scrape_state.last_reached_image_score is None:
                    raise ValueError("Last reached image score isn't set!")
                self.sort_associated_compare_filter_tag = CompareFilterTag("score", self.sort_tag.descending, True, str(scrape_state.last_reached_image_score))
            case _:
                raise NotImplementedError(f"Bound update for sort type \"{self.sort_tag.sort_type}\" is not implemented!")

    def to_search_string(self):
        tag_texts = [str(self.sort_tag)]
        for compare_filter_tag in self.compare_filter_tags:
            tag_texts.append(str(compare_filter_tag))
        if self.sort_associated_compare_filter_tag is not None:
            tag_texts.append(str(self.sort_associated_compare_filter_tag))
        tag_texts += self.general_tags
        return "+".join(urllib.parse.quote(tag_text, safe="") for tag_text in tag_texts)
