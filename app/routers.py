"""
DISTRIBUTED BLOCKCHAIN API ENDPOINTS
=====================================

API for the real distributed blockchain with consensus.
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Dict, Any, List, Optional

from .consensus import get_consensus, RaftConsensus
from .p2p_network import get_network, P2PNetwork
from .distributed_chain import get_distributed_blockchain, DistributedBlockchain

router = APIRouter(prefix="/distributed", tags=["distributed-blockchain"])


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


# === BLOCKCHAIN ENDPOINTS ===

@router.post("/transactions", response_model=TransactionResponse)
async def submit_transaction(
    request: TransactionRequest,
    blockchain: DistributedBlockchain = Depends(get_blockchain),
):
    """Submit a transaction to the distributed blockchain."""
    tx = {
        "tx_type": request.tx_type,
        **request.payload,
    }
    
    tx_hash = await blockchain.submit_transaction(tx)
    
    return TransactionResponse(tx_hash=tx_hash, status="pending")


@router.get("/blocks/latest", response_model=BlockResponse)
async def get_latest_block(
    blockchain: DistributedBlockchain = Depends(get_blockchain),
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
):
    """Get node status."""
    status = blockchain.get_status()
    return NodeStatusResponse(**status)


@router.get("/chain/verify")
async def verify_chain(
    blockchain: DistributedBlockchain = Depends(get_blockchain),
):
    """Verify chain integrity."""
    return blockchain.verify_chain()


@router.get("/state/{key}")
async def get_state(
    key: str,
    blockchain: DistributedBlockchain = Depends(get_blockchain),
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
):
    """Connect to a new peer."""
    success = await network._connect_to_peer(address, port)
    return {"success": success}
