from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, FileResponse

from .config import Settings
from .data import init_data_store
from .routes import facilities, segments, stats, health, events


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: load data
    settings = Settings()
    init_data_store(settings)
    print(f"Data directory: {settings.data_dir}")

    yield
    # Shutdown: nothing to clean up


app = FastAPI(
    title="EEA River Proximity API",
    description="API for querying European industrial facilities and their river proximity data",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
settings = Settings()
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(facilities.router)
app.include_router(segments.router)
app.include_router(stats.router)
app.include_router(health.router)
app.include_router(events.router)


# Frontend path
FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"


@app.get("/", include_in_schema=False)
def root():
    """Serve frontend map."""
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/api", include_in_schema=False)
def api_docs():
    """Redirect /api to API docs."""
    return RedirectResponse(url="/docs")
