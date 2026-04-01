"""RG External Blockchain — Distributed chain with Raft consensus and P2P network."""

import logging
import sys
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

logger = logging.getLogger(__name__)

# Optional shared imports for Docker compatibility
try:
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from shared.errors import setup_exception_handlers
    HAS_SHARED_ERRORS = True
except ImportError:
    HAS_SHARED_ERRORS = False
    setup_exception_handlers = None

from .routers import router
from .distributed_chain import get_distributed_blockchain


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: initialize distributed blockchain. Shutdown: stop it."""
    logger.info("RG External Blockchain starting...")
    blockchain = await get_distributed_blockchain()
    yield
    await blockchain.stop()
    logger.info("RG External Blockchain stopped")


app = FastAPI(
    title="RG External Blockchain",
    description="Distributed chain: Raft consensus, P2P network, block production, fork handling",
    version="0.1.0",
    lifespan=lifespan,
    redirect_slashes=False,
)

# Setup standardized exception handlers
if HAS_SHARED_ERRORS and setup_exception_handlers:
    setup_exception_handlers(app)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


@app.get("/")
async def root():
    return {"service": "rg-external-blockchain", "version": "0.1.0"}


@app.get("/health")
async def health():
    return {"status": "ok", "service": "rg-external-blockchain"}
