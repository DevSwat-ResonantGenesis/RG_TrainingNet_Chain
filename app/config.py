"""RG External Blockchain configuration."""

import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Service
    SERVICE_NAME: str = "rg-external-blockchain"
    SERVICE_VERSION: str = "0.1.0"

    # Node identity
    NODE_ID: str = os.getenv("NODE_ID", "")
    NODE_ADDRESS: str = os.getenv("NODE_ADDRESS", "0.0.0.0")

    # Consensus (Raft)
    CONSENSUS_PORT: int = int(os.getenv("CONSENSUS_PORT", "8500"))
    CONSENSUS_PEERS: str = os.getenv("CONSENSUS_PEERS", "[]")

    # P2P Network
    P2P_PORT: int = int(os.getenv("P2P_PORT", "8600"))
    P2P_BOOTSTRAP_NODES: str = os.getenv("P2P_BOOTSTRAP_NODES", "[]")
    P2P_MAX_PEERS: int = int(os.getenv("P2P_MAX_PEERS", "50"))

    # Block production
    BLOCK_TIME: int = int(os.getenv("BLOCK_TIME", "10"))
    MAX_TX_PER_BLOCK: int = int(os.getenv("MAX_TX_PER_BLOCK", "100"))

    # Chain
    CHAIN_ID: str = os.getenv("CHAIN_ID", "resonant-genesis-external-1")

    # Redis (for caching/pubsub)
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/6")

    class Config:
        env_file = ".env"


settings = Settings()
