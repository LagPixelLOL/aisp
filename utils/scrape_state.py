from typing import Optional
from aiohttp import ClientSession
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor

@dataclass
class ScrapeState:
    thread_pool: ClientSession
    session: ThreadPoolExecutor
    existing_image_ids: set[str] = field(default_factory=set)
    scraped_image_count: int = 0
    last_reached_image_id: Optional[str] = None
    last_reached_image_score: Optional[int] = None
    avg_query_time: list[float, int] = field(default_factory=lambda: [0.0, 0])
    avg_download_time: list[float, int] = field(default_factory=lambda: [0.0, 0])
