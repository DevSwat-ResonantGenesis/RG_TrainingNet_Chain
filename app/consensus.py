"""
RAFT CONSENSUS PROTOCOL - Real distributed consensus for blockchain.
"""

import asyncio
import logging
import random
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass, field
from enum import Enum
from uuid import uuid4

logger = logging.getLogger(__name__)


class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    index: int
    term: int
    command: Dict[str, Any]
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        return {"index": self.index, "term": self.term, "command": self.command, "timestamp": self.timestamp}
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LogEntry":
        return cls(index=data["index"], term=data["term"], command=data["command"], timestamp=data.get("timestamp", ""))


@dataclass
class NodeInfo:
    node_id: str
    address: str
    port: int
    last_heartbeat: float = 0
    next_index: int = 0
    match_index: int = 0
    
    @property
    def endpoint(self) -> str:
        return f"http://{self.address}:{self.port}"


class RaftConsensus:
    HEARTBEAT_INTERVAL = 0.5
    ELECTION_TIMEOUT_MIN = 1.5
    ELECTION_TIMEOUT_MAX = 3.0
    
    def __init__(self, node_id: str, address: str = "localhost", port: int = 8500, peers: List[Dict] = None):
        self.node_id = node_id
        self.address = address
        self.port = port
        self.current_term = 0
        self.voted_for = None
        self.log: List[LogEntry] = []
        self.commit_index = 0
        self.last_applied = 0
        self.state = NodeState.FOLLOWER
        self.leader_id = None
        self.peers: Dict[str, NodeInfo] = {}
        for p in (peers or []):
            pid = p.get("node_id", str(uuid4()))
            self.peers[pid] = NodeInfo(node_id=pid, address=p.get("address", "localhost"), port=p.get("port", 8500))
        self._election_timeout = random.uniform(self.ELECTION_TIMEOUT_MIN, self.ELECTION_TIMEOUT_MAX)
        self._last_heartbeat = time.time()
        self._running = False
        self._tasks = []
        self._on_commit = None
        self._client = None
    
    async def start(self):
        import httpx
        self._client = httpx.AsyncClient(timeout=2.0)
        self._running = True
        self._tasks.append(asyncio.create_task(self._election_loop()))
        logger.info(f"Raft node {self.node_id} started with {len(self.peers)} peers")
    
    async def stop(self):
        self._running = False
        for t in self._tasks:
            t.cancel()
        if self._client:
            await self._client.aclose()
    
    async def _election_loop(self):
        while self._running:
            await asyncio.sleep(0.1)
            if self.state != NodeState.LEADER and time.time() - self._last_heartbeat > self._election_timeout:
                await self._start_election()
    
    async def _start_election(self):
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self._election_timeout = random.uniform(self.ELECTION_TIMEOUT_MIN, self.ELECTION_TIMEOUT_MAX)
        self._last_heartbeat = time.time()
        votes = 1
        needed = (len(self.peers) + 1) // 2 + 1
        last_idx = len(self.log) - 1 if self.log else -1
        last_term = self.log[-1].term if self.log else 0
        
        for peer in self.peers.values():
            try:
                r = await self._client.post(f"{peer.endpoint}/distributed/raft/vote", json={
                    "term": self.current_term, "candidate": self.node_id, "last_idx": last_idx, "last_term": last_term
                })
                if r.status_code == 200:
                    data = r.json()
                    if data.get("granted"):
                        votes += 1
                    if data.get("term", 0) > self.current_term:
                        self.current_term = data["term"]
                        self.state = NodeState.FOLLOWER
                        return
            except:
                pass
        
        if votes >= needed and self.state == NodeState.CANDIDATE:
            await self._become_leader()
        else:
            self.state = NodeState.FOLLOWER
    
    async def _become_leader(self):
        self.state = NodeState.LEADER
        self.leader_id = self.node_id
        for p in self.peers.values():
            p.next_index = len(self.log)
            p.match_index = 0
        logger.info(f"Node {self.node_id} is LEADER for term {self.current_term}")
        self._tasks.append(asyncio.create_task(self._heartbeat_loop()))
    
    async def _heartbeat_loop(self):
        while self._running and self.state == NodeState.LEADER:
            await self._replicate()
            await asyncio.sleep(self.HEARTBEAT_INTERVAL)
    
    async def _replicate(self):
        for peer in self.peers.values():
            prev_idx = peer.next_index - 1
            prev_term = self.log[prev_idx].term if 0 <= prev_idx < len(self.log) else 0
            entries = self.log[peer.next_index:] if peer.next_index < len(self.log) else []
            try:
                r = await self._client.post(f"{peer.endpoint}/distributed/raft/append", json={
                    "term": self.current_term, "leader": self.node_id, "prev_idx": prev_idx,
                    "prev_term": prev_term, "entries": [e.to_dict() for e in entries], "commit": self.commit_index
                })
                if r.status_code == 200:
                    data = r.json()
                    if data.get("success"):
                        peer.match_index = data.get("match", len(self.log) - 1)
                        peer.next_index = peer.match_index + 1
                    else:
                        peer.next_index = max(0, peer.next_index - 1)
                    if data.get("term", 0) > self.current_term:
                        self.current_term = data["term"]
                        self.state = NodeState.FOLLOWER
            except:
                pass
        self._update_commit()
    
    def _update_commit(self):
        if self.state != NodeState.LEADER:
            return
        indices = sorted([p.match_index for p in self.peers.values()] + [len(self.log) - 1], reverse=True)
        majority = (len(self.peers) + 1) // 2
        if len(indices) >= majority:
            n = indices[majority - 1]
            if n > self.commit_index and n < len(self.log) and self.log[n].term == self.current_term:
                self.commit_index = n
                self._apply()
    
    def _apply(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            if self._on_commit and self.last_applied < len(self.log):
                self._on_commit(self.log[self.last_applied])
    
    async def handle_vote(self, term: int, candidate: str, last_idx: int, last_term: int) -> Dict:
        if term > self.current_term:
            self.current_term = term
            self.state = NodeState.FOLLOWER
            self.voted_for = None
        granted = False
        if term >= self.current_term and (self.voted_for is None or self.voted_for == candidate):
            my_idx = len(self.log) - 1 if self.log else -1
            my_term = self.log[-1].term if self.log else 0
            if last_term > my_term or (last_term == my_term and last_idx >= my_idx):
                granted = True
                self.voted_for = candidate
                self._last_heartbeat = time.time()
        return {"term": self.current_term, "granted": granted}
    
    async def handle_append(self, term: int, leader: str, prev_idx: int, prev_term: int, entries: List, commit: int) -> Dict:
        if term > self.current_term:
            self.current_term = term
            self.state = NodeState.FOLLOWER
            self.voted_for = None
        if term < self.current_term:
            return {"term": self.current_term, "success": False}
        self._last_heartbeat = time.time()
        self.leader_id = leader
        if self.state == NodeState.CANDIDATE:
            self.state = NodeState.FOLLOWER
        if prev_idx >= 0:
            if prev_idx >= len(self.log) or self.log[prev_idx].term != prev_term:
                return {"term": self.current_term, "success": False}
        for e in entries:
            entry = LogEntry.from_dict(e) if isinstance(e, dict) else e
            if entry.index < len(self.log):
                if self.log[entry.index].term != entry.term:
                    self.log = self.log[:entry.index]
                    self.log.append(entry)
            else:
                self.log.append(entry)
        if commit > self.commit_index:
            self.commit_index = min(commit, len(self.log) - 1)
            self._apply()
        return {"term": self.current_term, "success": True, "match": len(self.log) - 1}
    
    async def submit(self, command: Dict) -> bool:
        if self.state != NodeState.LEADER:
            return False
        entry = LogEntry(index=len(self.log), term=self.current_term, command=command)
        self.log.append(entry)
        await self._replicate()
        return True
    
    def set_on_commit(self, cb):
        self._on_commit = cb
    
    def get_status(self) -> Dict:
        return {"node_id": self.node_id, "state": self.state.value, "term": self.current_term,
                "leader": self.leader_id, "log_length": len(self.log), "commit": self.commit_index}


# Global instance
_consensus = None

async def get_consensus(node_id: str = None, peers: List = None) -> RaftConsensus:
    global _consensus
    if _consensus is None:
        import os
        node_id = node_id or os.getenv("NODE_ID", str(uuid4()))
        _consensus = RaftConsensus(node_id=node_id, peers=peers or [])
        await _consensus.start()
    return _consensus
