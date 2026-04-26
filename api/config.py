from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Data paths
    data_dir: Path = Path(__file__).resolve().parent.parent
    facilities_file: str = "river_data_facilities.geoparquet"
    segments_file: str = "river_data_segments.geoparquet"

    # Pagination
    default_page_size: int = 50
    max_page_size: int = 1000

    # CORS
    cors_origins: list[str] = ["*"]

    @property
    def facilities_path(self) -> Path:
        return self.data_dir / self.facilities_file

    @property
    def segments_path(self) -> Path:
        return self.data_dir / self.segments_file

    class Config:
        env_prefix = "EEA_"
