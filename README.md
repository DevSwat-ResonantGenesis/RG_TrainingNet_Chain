# RG_TrainingNet_Chain

The **Training Network's distributed blockchain** — Raft consensus, P2P gossip, block production, and fork handling. Coordinates distributed LLM training across the network.

> **Part of the Training Network** (3 repos):
> - **RG_TrainingNet_Chain** (this) — Raft consensus chain, block production
> - **RG_TrainingNet_Mining** — Gradient aggregation, sharded training, RGT rewards
> - **RG_TrainingNet_Lighthouse** — Peer discovery, network beacon

> **Not to be confused with:**
> - **RG_DSID_Blockchain** — Internal audit/governance ledger (separate chain, different purpose)
> - **RG_DSID_Node** — DSID execution layer (agent runtime, Base Sepolia anchoring)

## Architecture: Platform Chains

| Chain | Repo(s) | Purpose |
|-------|---------|---------|
| **DSID** (internal) | `RG_DSID_Blockchain` + `RG_DSID_Node` | Audit, governance, agent identity, ETH anchoring |
| **Training Network** (this) | `RG_TrainingNet_Chain` + `Mining` + `Lighthouse` | Distributed LLM training coordination, RGT mining |
| **Cross-chain** (Base/ETH) | — | Identity anchoring target, $RGT token |

## Components

| File | Purpose |
|------|---------|
| `consensus.py` | Raft consensus: leader election, log replication, heartbeats |
| `p2p_network.py` | TCP P2P: gossip discovery, block/tx propagation, training messages |
| `distributed_chain.py` | Full blockchain: block production, merkle trees, fork handling, state machine |
| `routers.py` | REST API endpoints for transactions, blocks, consensus, peers |
| `main.py` | FastAPI application entry point |
| `config.py` | Service configuration |

## Transaction Types

| tx_type | Description |
|---------|-------------|
| `transfer` | Value transfer between accounts |
| `set` | Generic state update |
| `agent_action` | Record agent action on chain |
| `training_gradient` | Verified training gradient from miner (provenance) |

## API Endpoints

```
POST /distributed/transactions         — Submit a transaction
GET  /distributed/blocks/latest        — Get latest block
GET  /distributed/blocks/{number}      — Get block by number
GET  /distributed/status               — Node status (chain height, consensus, peers)
GET  /distributed/chain/verify         — Verify chain integrity
GET  /distributed/state/{key}          — Get state value
POST /distributed/raft/vote            — Raft vote (inter-node)
POST /distributed/raft/append          — Raft append entries (inter-node)
GET  /distributed/raft/status          — Consensus status
GET  /distributed/peers                — Connected peers
POST /distributed/peers/connect        — Connect to new peer
GET  /health                           — Health check
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NODE_ID` | auto-generated | Unique node identifier |
| `CONSENSUS_PEERS` | `[]` | JSON list of peer configs |
| `P2P_PORT` | `8600` | P2P listen port |
| `P2P_BOOTSTRAP_NODES` | `[]` | Bootstrap nodes for discovery |
| `BLOCK_TIME` | `10` | Block production interval (seconds) |
| `MAX_TX_PER_BLOCK` | `100` | Max transactions per block |

## Running

```bash
docker build -t rg-external-blockchain .
docker run -p 8000:8000 -p 8600:8600 \
  -e NODE_ID=node-1 \
  -e CONSENSUS_PEERS='[{"node_id":"node-2","address":"10.0.0.2","port":8500}]' \
  rg-external-blockchain
```
