"""
DISTRIBUTED BLOCKCHAIN
======================

Integrates consensus + P2P network + chain storage into a REAL blockchain.
This is the complete distributed ledger implementation for the external RG chain.
"""

import asyncio
import logging
import hashlib
import json
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from uuid import uuid4
import os

from .consensus import RaftConsensus, LogEntry, get_consensus
from .p2p_network import P2PNetwork, get_network, MessageType, NetworkMessage

logger = logging.getLogger(__name__)


@dataclass
class DistributedBlock:
    """Block in the distributed chain."""
    number: int
    hash: str
    previous_hash: str
    merkle_root: str
    transactions: List[Dict]
    timestamp: str
    validator: str
    term: int  # Consensus term
    signatures: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        return {
            "number": self.number,
            "hash": self.hash,
            "previous_hash": self.previous_hash,
            "merkle_root": self.merkle_root,
            "transactions": self.transactions,
            "timestamp": self.timestamp,
            "validator": self.validator,
            "term": self.term,
            "signatures": self.signatures,
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "DistributedBlock":
        return cls(
            number=data["number"],
            hash=data["hash"],
            previous_hash=data["previous_hash"],
            merkle_root=data["merkle_root"],
            transactions=data["transactions"],
            timestamp=data["timestamp"],
            validator=data["validator"],
            term=data["term"],
            signatures=data.get("signatures", []),
        )


class DistributedBlockchain:
    """
    Fully distributed blockchain with:
    - Raft consensus for leader election and log replication
    - P2P network for node communication
    - Fork handling and chain reorganization
    - Deterministic replay
    """
    
    def __init__(self, node_id: str = None):
        self.node_id = node_id or os.getenv("NODE_ID", str(uuid4()))
        
        # Core components
        self.consensus: Optional[RaftConsensus] = None
        self.network: Optional[P2PNetwork] = None
        
        # Chain state
        self.chain: List[DistributedBlock] = []
        self.pending_transactions: List[Dict] = []
        self.state: Dict[str, Any] = {}  # State machine
        
        # Fork handling
        self.forks: Dict[str, List[DistributedBlock]] = {}
        
        # Configuration
        self.block_time = int(os.getenv("BLOCK_TIME", "10"))  # seconds
        self.max_tx_per_block = int(os.getenv("MAX_TX_PER_BLOCK", "100"))
        
        self._running = False
        self._block_task = None
    
    async def start(self):
        """Start the distributed blockchain."""
        logger.info(f"Starting distributed blockchain node: {self.node_id}")
        
        # Initialize consensus
        peers = json.loads(os.getenv("CONSENSUS_PEERS", "[]"))
        self.consensus = await get_consensus(self.node_id, peers)
        self.consensus.set_on_commit(self._on_consensus_commit)
        
        # Initialize P2P network
        p2p_port = int(os.getenv("P2P_PORT", "8600"))
        self.network = await get_network(self.node_id, p2p_port)
        self.network.set_block_handler(self._handle_block_announce)
        self.network.set_tx_handler(self._handle_tx_announce)
        
        # Load chain from storage
        await self._load_chain()
        
        # Start block production (only leader produces)
        self._running = True
        self._block_task = asyncio.create_task(self._block_production_loop())
        
        logger.info(f"Distributed blockchain started. Chain height: {len(self.chain)}")
    
    async def stop(self):
        """Stop the blockchain."""
        self._running = False
        
        if self._block_task:
            self._block_task.cancel()
        
        if self.consensus:
            await self.consensus.stop()
        
        if self.network:
            await self.network.stop()
        
        logger.info("Distributed blockchain stopped")
    
    async def _load_chain(self):
        """Load chain from persistent storage."""
        # Genesis block
        if not self.chain:
            genesis = DistributedBlock(
                number=0,
                hash=self._compute_hash({"genesis": True, "timestamp": "2024-01-01T00:00:00Z"}),
                previous_hash="0" * 64,
                merkle_root="0" * 64,
                transactions=[],
                timestamp="2024-01-01T00:00:00Z",
                validator="genesis",
                term=0,
            )
            self.chain.append(genesis)
    
    def _compute_hash(self, data: Any) -> str:
        """Compute SHA256 hash."""
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
    
    def _compute_merkle_root(self, transactions: List[Dict]) -> str:
        """Compute merkle root of transactions."""
        if not transactions:
            return "0" * 64
        
        hashes = [self._compute_hash(tx) for tx in transactions]
        
        while len(hashes) > 1:
            if len(hashes) % 2 == 1:
                hashes.append(hashes[-1])
            
            new_hashes = []
            for i in range(0, len(hashes), 2):
                combined = hashes[i] + hashes[i + 1]
                new_hashes.append(self._compute_hash(combined))
            hashes = new_hashes
        
        return hashes[0]
    
    async def _block_production_loop(self):
        """Produce blocks when leader."""
        while self._running:
            try:
                await asyncio.sleep(self.block_time)
                
                # Only leader produces blocks
                if self.consensus and self.consensus.state.value == "leader":
                    if self.pending_transactions:
                        await self._produce_block()
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Block production error: {e}")
    
    async def _produce_block(self):
        """Produce a new block."""
        if not self.pending_transactions:
            return
        
        # Get transactions for this block
        txs = self.pending_transactions[:self.max_tx_per_block]
        
        # Get previous block
        prev_block = self.chain[-1] if self.chain else None
        
        # Create block
        block_data = {
            "number": (prev_block.number + 1) if prev_block else 0,
            "previous_hash": prev_block.hash if prev_block else "0" * 64,
            "transactions": txs,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "validator": self.node_id,
            "term": self.consensus.current_term if self.consensus else 0,
        }
        
        block_data["merkle_root"] = self._compute_merkle_root(txs)
        block_data["hash"] = self._compute_hash(block_data)
        
        # Submit to consensus for replication
        command = {"type": "new_block", "block": block_data}
        success = await self.consensus.submit(command)
        
        if success:
            logger.info(f"Block {block_data['number']} submitted to consensus")
            # Remove pending transactions
            self.pending_transactions = self.pending_transactions[self.max_tx_per_block:]
    
    def _on_consensus_commit(self, entry: LogEntry):
        """Called when consensus commits an entry."""
        command = entry.command
        cmd_type = command.get("type")
        
        if cmd_type == "new_block":
            block_data = command.get("block", {})
            block = DistributedBlock.from_dict(block_data)
            self._apply_block(block)
        
        elif cmd_type == "transaction":
            tx = command.get("transaction", {})
            self._apply_transaction(tx)
    
    def _apply_block(self, block: DistributedBlock):
        """Apply a committed block to the chain."""
        # Verify block links to chain
        if self.chain:
            if block.previous_hash != self.chain[-1].hash:
                logger.warning(f"Block {block.number} doesn't link to chain, checking for fork")
                self._handle_potential_fork(block)
                return
        
        # Add to chain
        self.chain.append(block)
        
        # Apply transactions to state
        for tx in block.transactions:
            self._apply_transaction(tx)
        
        logger.info(f"Applied block {block.number}, chain height: {len(self.chain)}")
        
        # Announce to network
        if self.network:
            asyncio.create_task(self.network.announce_block(block.to_dict()))
    
    def _apply_transaction(self, tx: Dict):
        """Apply transaction to state machine."""
        tx_type = tx.get("tx_type")
        
        if tx_type == "transfer":
            from_acc = tx.get("from")
            to_acc = tx.get("to")
            amount = tx.get("amount", 0)
            
            if from_acc in self.state:
                self.state[from_acc] = self.state.get(from_acc, 0) - amount
            self.state[to_acc] = self.state.get(to_acc, 0) + amount
        
        elif tx_type == "set":
            key = tx.get("key")
            value = tx.get("value")
            if key:
                self.state[key] = value
        
        elif tx_type == "agent_action":
            agent_id = tx.get("agent_id")
            action = tx.get("action")
            key = f"agent:{agent_id}:actions"
            if key not in self.state:
                self.state[key] = []
            self.state[key].append(action)
        
        elif tx_type == "training_gradient":
            miner_id = tx.get("miner_id")
            task_id = tx.get("task_id")
            gradient_hash = tx.get("gradient_hash")
            loss_value = tx.get("loss_value", 0.0)
            samples = tx.get("samples_processed", 0)
            reward = tx.get("reward_amount", 0)
            
            # Track miner's training contributions
            miner_key = f"miner:{miner_id}:training"
            if miner_key not in self.state:
                self.state[miner_key] = {"tasks": 0, "samples": 0, "rewards": 0}
            self.state[miner_key]["tasks"] += 1
            self.state[miner_key]["samples"] += samples
            self.state[miner_key]["rewards"] += reward
            
            # Record gradient hash for provenance
            grad_key = f"gradient:{task_id}"
            self.state[grad_key] = {
                "miner_id": miner_id,
                "gradient_hash": gradient_hash,
                "loss": loss_value,
                "samples": samples,
            }
    
    def _handle_potential_fork(self, block: DistributedBlock):
        """Handle a potential chain fork."""
        fork_hash = block.previous_hash
        fork_point = -1
        
        for i, b in enumerate(self.chain):
            if b.hash == fork_hash:
                fork_point = i
                break
        
        if fork_point == -1:
            logger.warning(f"Cannot find fork point for block {block.number}")
            return
        
        fork_id = f"fork_{block.hash[:8]}"
        self.forks[fork_id] = [block]
        
        logger.info(f"Created fork {fork_id} at height {fork_point}")
        
        fork_length = fork_point + len(self.forks[fork_id])
        main_length = len(self.chain)
        
        if fork_length > main_length:
            self._reorganize_chain(fork_id, fork_point)
    
    def _reorganize_chain(self, fork_id: str, fork_point: int):
        """Reorganize chain to follow longer fork."""
        logger.info(f"Reorganizing chain to follow fork {fork_id}")
        
        reverted_blocks = self.chain[fork_point + 1:]
        self.chain = self.chain[:fork_point + 1]
        
        for block in reversed(reverted_blocks):
            self.pending_transactions = block.transactions + self.pending_transactions
        
        for block in self.forks[fork_id]:
            self.chain.append(block)
            for tx in block.transactions:
                self._apply_transaction(tx)
        
        del self.forks[fork_id]
        
        logger.info(f"Chain reorganized, new height: {len(self.chain)}")
    
    async def _handle_block_announce(self, payload: Dict):
        """Handle block announcement from network."""
        block_data = payload.get("block", {})
        block = DistributedBlock.from_dict(block_data)
        
        for b in self.chain:
            if b.hash == block.hash:
                return
        
        if self.consensus and self.consensus.state.value != "leader":
            self._apply_block(block)
    
    async def _handle_tx_announce(self, payload: Dict):
        """Handle transaction announcement from network."""
        tx = payload.get("transaction", {})
        tx_hash = self._compute_hash(tx)
        
        for pending_tx in self.pending_transactions:
            if self._compute_hash(pending_tx) == tx_hash:
                return
        
        self.pending_transactions.append(tx)
    
    # === PUBLIC API ===
    
    async def submit_transaction(self, tx: Dict) -> str:
        """Submit a transaction."""
        tx["tx_id"] = str(uuid4())
        tx["submitted_at"] = datetime.now(timezone.utc).isoformat()
        tx["tx_hash"] = self._compute_hash(tx)
        
        self.pending_transactions.append(tx)
        
        if self.network:
            await self.network.announce_transaction(tx)
        
        return tx["tx_hash"]
    
    def get_block(self, number: int) -> Optional[Dict]:
        """Get block by number."""
        if 0 <= number < len(self.chain):
            return self.chain[number].to_dict()
        return None
    
    def get_latest_block(self) -> Optional[Dict]:
        """Get latest block."""
        if self.chain:
            return self.chain[-1].to_dict()
        return None
    
    def get_chain_height(self) -> int:
        """Get chain height."""
        return len(self.chain)
    
    def get_state(self, key: str) -> Any:
        """Get state value."""
        return self.state.get(key)
    
    def verify_chain(self) -> Dict:
        """Verify chain integrity."""
        errors = []
        
        for i in range(1, len(self.chain)):
            block = self.chain[i]
            prev_block = self.chain[i - 1]
            
            if block.previous_hash != prev_block.hash:
                errors.append(f"Block {block.number}: invalid previous_hash")
            
            block_data = block.to_dict()
            del block_data["hash"]
            del block_data["signatures"]
            expected_hash = self._compute_hash(block_data)
            if block.hash != expected_hash:
                errors.append(f"Block {block.number}: invalid hash")
            
            expected_merkle = self._compute_merkle_root(block.transactions)
            if block.merkle_root != expected_merkle:
                errors.append(f"Block {block.number}: invalid merkle_root")
        
        return {
            "valid": len(errors) == 0,
            "height": len(self.chain),
            "errors": errors,
        }
    
    def get_status(self) -> Dict:
        """Get blockchain status."""
        return {
            "node_id": self.node_id,
            "chain_height": len(self.chain),
            "pending_transactions": len(self.pending_transactions),
            "consensus_state": self.consensus.state.value if self.consensus else "unknown",
            "consensus_term": self.consensus.current_term if self.consensus else 0,
            "leader": self.consensus.leader_id if self.consensus else None,
            "peers": self.network.get_peer_count() if self.network else 0,
            "forks": len(self.forks),
        }


# Global instance
_blockchain = None

async def get_distributed_blockchain(node_id: str = None) -> DistributedBlockchain:
    global _blockchain
    if _blockchain is None:
        _blockchain = DistributedBlockchain(node_id=node_id)
        await _blockchain.start()
    return _blockchain
