"""
P2P NETWORK PROTOCOL
====================

Real peer-to-peer networking for distributed blockchain nodes.
Enables node discovery, message propagation, and chain synchronization.
"""

import asyncio
import logging
import json
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Set, Callable
from dataclasses import dataclass, field
from enum import Enum
from uuid import uuid4
import os

logger = logging.getLogger(__name__)


class MessageType(Enum):
    PING = "ping"
    PONG = "pong"
    DISCOVER = "discover"
    PEERS = "peers"
    BLOCK_ANNOUNCE = "block_announce"
    BLOCK_REQUEST = "block_request"
    BLOCK_RESPONSE = "block_response"
    TX_ANNOUNCE = "tx_announce"
    TX_REQUEST = "tx_request"
    TX_RESPONSE = "tx_response"
    CHAIN_SYNC = "chain_sync"
    CONSENSUS = "consensus"
    TRAINING_TASK = "training_task"
    GRADIENT_SUBMIT = "gradient_submit"
    WEIGHT_SHARD = "weight_shard"


@dataclass
class Peer:
    node_id: str
    address: str
    port: int
    last_seen: float = 0
    latency_ms: float = 0
    version: str = "1.0"
    capabilities: List[str] = field(default_factory=list)
    
    @property
    def endpoint(self) -> str:
        return f"{self.address}:{self.port}"


@dataclass
class NetworkMessage:
    msg_type: MessageType
    sender: str
    payload: Dict[str, Any]
    msg_id: str = field(default_factory=lambda: str(uuid4()))
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    ttl: int = 5  # Max hops
    
    def to_dict(self) -> Dict:
        return {
            "msg_type": self.msg_type.value,
            "sender": self.sender,
            "payload": self.payload,
            "msg_id": self.msg_id,
            "timestamp": self.timestamp,
            "ttl": self.ttl,
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "NetworkMessage":
        return cls(
            msg_type=MessageType(data["msg_type"]),
            sender=data["sender"],
            payload=data["payload"],
            msg_id=data.get("msg_id", str(uuid4())),
            timestamp=data.get("timestamp", ""),
            ttl=data.get("ttl", 5),
        )


class P2PNetwork:
    """
    Peer-to-peer network for blockchain nodes.
    
    Features:
    - Node discovery via gossip
    - Block and transaction propagation
    - Chain synchronization
    - NAT traversal (basic)
    """
    
    MAX_PEERS = 50
    PING_INTERVAL = 30
    DISCOVERY_INTERVAL = 60
    
    def __init__(self, node_id: str, listen_port: int = 8600, bootstrap_nodes: List[Dict] = None):
        self.node_id = node_id
        self.listen_port = listen_port
        self.bootstrap_nodes = bootstrap_nodes or []
        
        self.peers: Dict[str, Peer] = {}
        self.seen_messages: Set[str] = set()
        self._max_seen = 10000
        
        self._server = None
        self._running = False
        self._tasks = []
        
        # Handlers
        self._block_handler: Optional[Callable] = None
        self._tx_handler: Optional[Callable] = None
        self._consensus_handler: Optional[Callable] = None
        self._training_task_handler: Optional[Callable] = None
        self._gradient_handler: Optional[Callable] = None
        self._weight_shard_handler: Optional[Callable] = None
    
    async def start(self):
        """Start P2P network."""
        self._running = True
        
        # Start TCP server
        self._server = await asyncio.start_server(
            self._handle_connection,
            "0.0.0.0",
            self.listen_port,
        )
        
        # Start background tasks
        self._tasks.append(asyncio.create_task(self._ping_loop()))
        self._tasks.append(asyncio.create_task(self._discovery_loop()))
        
        # Connect to bootstrap nodes
        for node in self.bootstrap_nodes:
            await self._connect_to_peer(node.get("address"), node.get("port", 8600))
        
        logger.info(f"P2P network started on port {self.listen_port}")
    
    async def stop(self):
        """Stop P2P network."""
        self._running = False
        
        for task in self._tasks:
            task.cancel()
        
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        
        logger.info("P2P network stopped")
    
    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle incoming connection."""
        addr = writer.get_extra_info('peername')
        
        try:
            while self._running:
                data = await asyncio.wait_for(reader.readline(), timeout=60)
                if not data:
                    break
                
                msg = NetworkMessage.from_dict(json.loads(data.decode()))
                response = await self._handle_message(msg)
                
                if response:
                    writer.write(json.dumps(response.to_dict()).encode() + b'\n')
                    await writer.drain()
                    
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            logger.debug(f"Connection error from {addr}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()
    
    async def _handle_message(self, msg: NetworkMessage) -> Optional[NetworkMessage]:
        """Handle incoming message."""
        # Deduplicate
        if msg.msg_id in self.seen_messages:
            return None
        self.seen_messages.add(msg.msg_id)
        if len(self.seen_messages) > self._max_seen:
            self.seen_messages = set(list(self.seen_messages)[-5000:])
        
        if msg.msg_type == MessageType.PING:
            return NetworkMessage(MessageType.PONG, self.node_id, {"node_id": self.node_id})
        
        elif msg.msg_type == MessageType.DISCOVER:
            peers_list = [{"node_id": p.node_id, "address": p.address, "port": p.port} 
                         for p in list(self.peers.values())[:20]]
            return NetworkMessage(MessageType.PEERS, self.node_id, {"peers": peers_list})
        
        elif msg.msg_type == MessageType.PEERS:
            for p in msg.payload.get("peers", []):
                if p["node_id"] != self.node_id and p["node_id"] not in self.peers:
                    await self._connect_to_peer(p["address"], p["port"])
        
        elif msg.msg_type == MessageType.BLOCK_ANNOUNCE:
            if self._block_handler:
                await self._block_handler(msg.payload)
            # Propagate
            if msg.ttl > 0:
                msg.ttl -= 1
                await self.broadcast(msg)
        
        elif msg.msg_type == MessageType.TX_ANNOUNCE:
            if self._tx_handler:
                await self._tx_handler(msg.payload)
            if msg.ttl > 0:
                msg.ttl -= 1
                await self.broadcast(msg)
        
        elif msg.msg_type == MessageType.CONSENSUS:
            if self._consensus_handler:
                return await self._consensus_handler(msg)
        
        elif msg.msg_type == MessageType.TRAINING_TASK:
            if self._training_task_handler:
                await self._training_task_handler(msg.payload)
            if msg.ttl > 0:
                msg.ttl -= 1
                await self.broadcast(msg)
        
        elif msg.msg_type == MessageType.GRADIENT_SUBMIT:
            if self._gradient_handler:
                await self._gradient_handler(msg.payload)
        
        elif msg.msg_type == MessageType.WEIGHT_SHARD:
            if self._weight_shard_handler:
                await self._weight_shard_handler(msg.payload)
        
        return None
    
    async def _connect_to_peer(self, address: str, port: int) -> bool:
        """Connect to a peer."""
        if len(self.peers) >= self.MAX_PEERS:
            return False
        
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(address, port),
                timeout=5.0
            )
            
            # Send ping
            ping = NetworkMessage(MessageType.PING, self.node_id, {"node_id": self.node_id})
            writer.write(json.dumps(ping.to_dict()).encode() + b'\n')
            await writer.drain()
            
            # Wait for pong
            data = await asyncio.wait_for(reader.readline(), timeout=5.0)
            response = NetworkMessage.from_dict(json.loads(data.decode()))
            
            if response.msg_type == MessageType.PONG:
                peer_id = response.payload.get("node_id", str(uuid4()))
                self.peers[peer_id] = Peer(
                    node_id=peer_id,
                    address=address,
                    port=port,
                    last_seen=asyncio.get_event_loop().time(),
                )
                logger.info(f"Connected to peer {peer_id} at {address}:{port}")
                
            writer.close()
            await writer.wait_closed()
            return True
            
        except Exception as e:
            logger.debug(f"Failed to connect to {address}:{port}: {e}")
            return False
    
    async def _ping_loop(self):
        """Periodic ping to maintain connections."""
        while self._running:
            await asyncio.sleep(self.PING_INTERVAL)
            
            dead_peers = []
            for peer_id, peer in list(self.peers.items()):
                try:
                    reader, writer = await asyncio.wait_for(
                        asyncio.open_connection(peer.address, peer.port),
                        timeout=5.0
                    )
                    
                    ping = NetworkMessage(MessageType.PING, self.node_id, {})
                    writer.write(json.dumps(ping.to_dict()).encode() + b'\n')
                    await writer.drain()
                    
                    await asyncio.wait_for(reader.readline(), timeout=5.0)
                    peer.last_seen = asyncio.get_event_loop().time()
                    
                    writer.close()
                    await writer.wait_closed()
                except:
                    dead_peers.append(peer_id)
            
            for pid in dead_peers:
                del self.peers[pid]
                logger.debug(f"Removed dead peer {pid}")
    
    async def _discovery_loop(self):
        """Periodic peer discovery."""
        while self._running:
            await asyncio.sleep(self.DISCOVERY_INTERVAL)
            
            if len(self.peers) < 10:
                for peer in list(self.peers.values())[:5]:
                    await self._send_to_peer(peer, NetworkMessage(
                        MessageType.DISCOVER, self.node_id, {}
                    ))
    
    async def _send_to_peer(self, peer: Peer, msg: NetworkMessage) -> Optional[NetworkMessage]:
        """Send message to a specific peer."""
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(peer.address, peer.port),
                timeout=5.0
            )
            
            writer.write(json.dumps(msg.to_dict()).encode() + b'\n')
            await writer.drain()
            
            data = await asyncio.wait_for(reader.readline(), timeout=5.0)
            writer.close()
            await writer.wait_closed()
            
            if data:
                return NetworkMessage.from_dict(json.loads(data.decode()))
            return None
            
        except Exception as e:
            logger.debug(f"Failed to send to {peer.endpoint}: {e}")
            return None
    
    async def broadcast(self, msg: NetworkMessage):
        """Broadcast message to all peers."""
        tasks = []
        for peer in list(self.peers.values()):
            tasks.append(self._send_to_peer(peer, msg))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def announce_block(self, block: Dict):
        """Announce new block to network."""
        msg = NetworkMessage(MessageType.BLOCK_ANNOUNCE, self.node_id, {"block": block})
        await self.broadcast(msg)
    
    async def announce_transaction(self, tx: Dict):
        """Announce new transaction to network."""
        msg = NetworkMessage(MessageType.TX_ANNOUNCE, self.node_id, {"transaction": tx})
        await self.broadcast(msg)
    
    async def broadcast_training_task(self, task: Dict):
        """Broadcast a training task to all miner peers."""
        msg = NetworkMessage(MessageType.TRAINING_TASK, self.node_id, {"task": task})
        await self.broadcast(msg)
    
    async def submit_gradient(self, peer: Peer, gradient_payload: Dict):
        """Submit compressed gradient to a specific validator peer."""
        msg = NetworkMessage(MessageType.GRADIENT_SUBMIT, self.node_id, gradient_payload)
        return await self._send_to_peer(peer, msg)
    
    async def send_weight_shard(self, peer: Peer, shard_payload: Dict):
        """Send model weight shard to a specific miner peer."""
        msg = NetworkMessage(MessageType.WEIGHT_SHARD, self.node_id, shard_payload)
        return await self._send_to_peer(peer, msg)
    
    def set_block_handler(self, handler: Callable):
        self._block_handler = handler
    
    def set_tx_handler(self, handler: Callable):
        self._tx_handler = handler
    
    def set_consensus_handler(self, handler: Callable):
        self._consensus_handler = handler
    
    def set_training_task_handler(self, handler: Callable):
        self._training_task_handler = handler
    
    def set_gradient_handler(self, handler: Callable):
        self._gradient_handler = handler
    
    def set_weight_shard_handler(self, handler: Callable):
        self._weight_shard_handler = handler
    
    def get_peer_count(self) -> int:
        return len(self.peers)
    
    def get_peers(self) -> List[Dict]:
        return [{"node_id": p.node_id, "address": p.address, "port": p.port} for p in self.peers.values()]


# Global instance
_network = None

async def get_network(node_id: str = None, port: int = None) -> P2PNetwork:
    global _network
    if _network is None:
        import json as _json
        node_id = node_id or os.getenv("NODE_ID", str(uuid4()))
        port = port or int(os.getenv("P2P_PORT", "8600"))
        bootstrap = _json.loads(os.getenv("P2P_BOOTSTRAP_NODES", "[]"))
        _network = P2PNetwork(node_id=node_id, listen_port=port, bootstrap_nodes=bootstrap)
        await _network.start()
    return _network
