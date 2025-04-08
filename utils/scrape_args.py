from typing import Optional, Any
from dataclasses import dataclass

@dataclass
class ScrapeArgs:
    target: Any
    width: Optional[int] = None
    height: Optional[int] = None
    convert_to_avif: bool = False
    use_low_quality: bool = False
    min_tags: int = 0
    max_scrape_count: Optional[int] = None
    tag_type_dict: Optional[dict[str, str]] = None
