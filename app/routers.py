"""
ResonantGenesis Blockchain API Endpoints
=========================================

API for the ResonantGenesis distributed blockchain with Raft consensus.
Handles identity anchoring, training gradient records, and $RGT token ledger.
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Dict, Any, List, Optional

from .consensus import get_consensus, RaftConsensus
from .p2p_network import get_network, P2PNetwork
from .distributed_chain import get_distributed_blockchain, DistributedBlockchain
from .auth_middleware import (
    AuthenticatedUser,
    get_current_user,
    check_rate_limit,
)

router = APIRouter(prefix="/distributed", tags=["distributed-blockchain"])
identity_router = APIRouter(prefix="/identity", tags=["identity"])


class TransactionRequest(BaseModel):
    tx_type: str
    payload: Dict[str, Any]


class TransactionResponse(BaseModel):
    tx_hash: str
    status: str


class BlockResponse(BaseModel):
    number: int
    hash: str
    previous_hash: str
    merkle_root: str
    transaction_count: int
    timestamp: str
    validator: str


class NodeStatusResponse(BaseModel):
    node_id: str
    chain_height: int
    pending_transactions: int
    consensus_state: str
    consensus_term: int
    leader: Optional[str]
    peers: int


class ConsensusVoteRequest(BaseModel):
    term: int
    candidate: str
    last_idx: int
    last_term: int


class ConsensusAppendRequest(BaseModel):
    term: int
    leader: str
    prev_idx: int
    prev_term: int
    entries: List[Dict]
    commit: int


# Dependency
async def get_blockchain() -> DistributedBlockchain:
    return await get_distributed_blockchain()


async def get_consensus_node() -> RaftConsensus:
    return await get_consensus()


async def get_p2p() -> P2PNetwork:
    return await get_network()


# === IDENTITY ENDPOINTS (called by auth_service on registration) ===

class IdentityRegisterRequest(BaseModel):
    user_id: str
    crypto_hash: str
    user_hash: str
    universe_id: str
    email: Optional[str] = None


class IdentityResponse(BaseModel):
    user_id: str
    crypto_hash: str
    user_hash: str
    universe_id: str
    tx_hash: str
    block_anchored: bool
    registered_at: str


@identity_router.post("/register", response_model=IdentityResponse)
async def register_identity(
    payload: IdentityRegisterRequest,
    blockchain: DistributedBlockchain = Depends(get_blockchain),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """
    Anchor a user's cryptographic identity on the ResonantGenesis Blockchain.
    Called by auth_service during user registration.

    Creates an immutable on-chain record binding:
      - user_id (platform UUID)
      - crypto_hash (SHA-256 blockchain identity)
      - user_hash (Hash Sphere semantic identity)
      - universe_id (Deterministic Anchor Universe)
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()

    tx = {
        "tx_type": "identity_register",
        "user_id": payload.user_id,
        "crypto_hash": payload.crypto_hash,
        "user_hash": payload.user_hash,
        "universe_id": payload.universe_id,
        "email_hash": __import__("hashlib").sha256(
            (payload.email or "").encode()
        ).hexdigest() if payload.email else None,
        "registered_at": now,
    }

    tx_hash = await blockchain.submit_transaction(tx)

    return IdentityResponse(
        user_id=payload.user_id,
        crypto_hash=payload.crypto_hash,
        user_hash=payload.user_hash,
        universe_id=payload.universe_id,
        tx_hash=tx_hash,
        block_anchored=False,  # Will be anchored in next block
        registered_at=now,
    )


@identity_router.get("/{user_id}", response_model=Optional[IdentityResponse])
async def get_identity(
    user_id: str,
    blockchain: DistributedBlockchain = Depends(get_blockchain),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """
    Look up a user's on-chain identity by user_id.
    Searches the blockchain state and transaction history.
    """
    # Check state machine first (fast path)
    state_key = f"identity:{user_id}"
    identity = blockchain.get_state(state_key)
    if identity:
        return IdentityResponse(
            user_id=identity.get("user_id", user_id),
            crypto_hash=identity.get("crypto_hash", ""),
            user_hash=identity.get("user_hash", ""),
            universe_id=identity.get("universe_id", ""),
            tx_hash=identity.get("tx_hash", ""),
            block_anchored=True,
            registered_at=identity.get("registered_at", ""),
        )

    # Scan pending transactions
    for tx in blockchain.pending_transactions:
        if tx.get("tx_type") == "identity_register" and tx.get("user_id") == user_id:
            return IdentityResponse(
                user_id=tx["user_id"],
                crypto_hash=tx.get("crypto_hash", ""),
                user_hash=tx.get("user_hash", ""),
                universe_id=tx.get("universe_id", ""),
                tx_hash=tx.get("tx_hash", ""),
                block_anchored=False,
                registered_at=tx.get("registered_at", ""),
            )

    raise HTTPException(status_code=404, detail="Identity not found on chain")


# === BLOCKCHAIN ENDPOINTS ===

@router.post("/transactions", response_model=TransactionResponse)
async def submit_transaction(
    request: TransactionRequest,
    blockchain: DistributedBlockchain = Depends(get_blockchain),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """Submit a transaction to the distributed blockchain."""
    check_rate_limit(user.user_id or "anon", "gradient_submit")
    tx = {
        "tx_type": request.tx_type,
        **request.payload,
    }
    
    tx_hash = await blockchain.submit_transaction(tx)
    
    return TransactionResponse(tx_hash=tx_hash, status="pending")


@router.get("/blocks/latest", response_model=BlockResponse)
async def get_latest_block(
    blockchain: DistributedBlockchain = Depends(get_blockchain),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """Get the latest block."""
    block = blockchain.get_latest_block()
    if not block:
        raise HTTPException(status_code=404, detail="No blocks yet")
    
    return BlockResponse(
        number=block["number"],
        hash=block["hash"],
        previous_hash=block["previous_hash"],
        merkle_root=block["merkle_root"],
        transaction_count=len(block["transactions"]),
        timestamp=block["timestamp"],
        validator=block["validator"],
    )


@router.get("/blocks/{number}", response_model=BlockResponse)
async def get_block(
    number: int,
    blockchain: DistributedBlockchain = Depends(get_blockchain),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """Get a block by number."""
    block = blockchain.get_block(number)
    if not block:
        raise HTTPException(status_code=404, detail="Block not found")
    
    return BlockResponse(
        number=block["number"],
        hash=block["hash"],
        previous_hash=block["previous_hash"],
        merkle_root=block["merkle_root"],
        transaction_count=len(block["transactions"]),
        timestamp=block["timestamp"],
        validator=block["validator"],
    )


@router.get("/status", response_model=NodeStatusResponse)
async def get_node_status(
    blockchain: DistributedBlockchain = Depends(get_blockchain),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """Get node status."""
    status = blockchain.get_status()
    return NodeStatusResponse(**status)


@router.get("/chain/verify")
async def verify_chain(
    blockchain: DistributedBlockchain = Depends(get_blockchain),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """Verify chain integrity."""
    return blockchain.verify_chain()


@router.get("/state/{key}")
async def get_state(
    key: str,
    blockchain: DistributedBlockchain = Depends(get_blockchain),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """Get state value."""
    value = blockchain.get_state(key)
    return {"key": key, "value": value}


# === CONSENSUS ENDPOINTS (for inter-node communication) ===

@router.post("/raft/vote")
async def handle_vote_request(
    request: ConsensusVoteRequest,
    consensus: RaftConsensus = Depends(get_consensus_node),
):
    """Handle Raft vote request."""
    result = await consensus.handle_vote(
        term=request.term,
        candidate=request.candidate,
        last_idx=request.last_idx,
        last_term=request.last_term,
    )
    return result


@router.post("/raft/append")
async def handle_append_entries(
    request: ConsensusAppendRequest,
    consensus: RaftConsensus = Depends(get_consensus_node),
):
    """Handle Raft append entries."""
    result = await consensus.handle_append(
        term=request.term,
        leader=request.leader,
        prev_idx=request.prev_idx,
        prev_term=request.prev_term,
        entries=request.entries,
        commit=request.commit,
    )
    return result


@router.get("/raft/status")
async def get_consensus_status(
    consensus: RaftConsensus = Depends(get_consensus_node),
):
    """Get consensus status."""
    return consensus.get_status()


# === P2P NETWORK ENDPOINTS ===

@router.get("/peers")
async def get_peers(
    network: P2PNetwork = Depends(get_p2p),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """Get connected peers."""
    return {
        "count": network.get_peer_count(),
        "peers": network.get_peers(),
    }


@router.post("/peers/connect")
async def connect_to_peer(
    address: str,
    port: int = 8600,
    network: P2PNetwork = Depends(get_p2p),
    user: AuthenticatedUser = Depends(get_current_user),
):
    """Connect to a new peer."""
    if not user.is_admin() and user.auth_method != "dev":
        raise HTTPException(status_code=403, detail="Admin role required to connect peers")
    success = await network._connect_to_peer(address, port)
    return {"success": success}
