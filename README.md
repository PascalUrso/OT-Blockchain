# OT-Blockchain

A research prototype for **blockchain-backed collaborative text editing** using **Operational Transformation (OT)** on [Hyperledger Fabric](https://www.hyperledger.org/projects/fabric).

## Overview

This project explores how a permissioned blockchain can serve as a shared, tamper-evident operation log for real-time collaborative document editing. Instead of relying on a central server to serialize and broadcast edits, all operations are committed as transactions on the Fabric ledger. Each client independently replays the committed operation log and applies OT to reconcile concurrent edits.

```
Client A ──┐                        ┌── Client B
           │  SubmitOp (tx)         │
           ▼                        ▼
    ┌──────────────────────────────────┐
    │       Hyperledger Fabric         │
    │  (append-only operation log)     │
    └──────────────────────────────────┘
           │  Block event / QueryOpsAfter
           ▼
    OT transform + apply → local view
```

## Repository Structure

```
ot-collab-java/
├── chaincode-java/          # Fabric chaincode (smart contract)
│   └── src/main/java/org/hyperledger/fabric/samples/assettransfer/
│       ├── AssetTransfer.java   # Chaincode entry point (InitDoc, SubmitOp, QueryOpsAfter…)
│       ├── Asset.java           # Document snapshot model
│       ├── Operation.java       # OT operation model (insert / delete / update)
│       ├── OperationRecord.java # On-chain log entry (operation + tx metadata)
│       └── OperationType.java   # Operation type enum
│
└── application-gateway-java/   # Client application (Fabric Gateway SDK)
    └── src/main/java/
        ├── App.java             # Main client: interactive REPL, OT engine, block listener
        ├── Connections.java     # gRPC + identity setup for the Fabric Gateway
        ├── DocumentState.java   # Local snapshot of the committed document
        ├── OTEngine.java        # Operational Transformation logic (transform + apply)
        ├── Operation.java       # Client-side operation model
        ├── OperationRecord.java # Client-side log entry model
        └── OperationType.java   # Operation type enum
```

## How It Works

1. **InitDoc** — a client creates a document on-chain (idempotent; subsequent calls are ignored).
2. **SubmitOp** — each edit is submitted as a Fabric transaction. The chaincode appends it to the document's operation log using a deterministically ordered key (based on the transaction timestamp).
3. **Block listener** — each client subscribes to block events. On every new block, it calls `QueryOpsAfter` to fetch new committed operations.
4. **OT reconciliation** — the client replays new remote ops through the OT transform pipeline against its local pending operations, updating both the committed view and the local view.

## Prerequisites

- Java 11+
- Gradle 7+
- A running Hyperledger Fabric network (e.g. the [fabric-samples test network](https://github.com/hyperledger/fabric-samples))
- The chaincode deployed under the name `otcollab` on channel `mychannel`

## Getting Started

### 1. Deploy the chaincode

```bash
cd chaincode-java
./gradlew build
# Then package and deploy via the Fabric peer CLI as usual
```

### 2. Run the client

```bash
cd application-gateway-java
./gradlew run
```

The client prompts for a `client_id` and a `doc_id`, then enters an interactive menu for staging and submitting operations.

### Interactive menu

```
1) INSERT(local pending)
2) DELETE(local pending)
3) UPDATE(local pending)
4) submit 1 local pending
5) submit all local pending
6) manually sync with chain
8) exit
```

## Key Design Decisions

- **No central OT server** — the blockchain acts as the ordered log; OT convergence is computed client-side.
- **MVCC conflict handling** — on `MVCC_READ_CONFLICT`, the client re-syncs from the chain and retries the submission (up to `MAX_SUBMIT_RETRIES = 3` times).
- **Ack-based buffer trimming** — each submitted operation carries an `ack` counter so the server-side buffer for the sender can be trimmed, keeping the OT state compact.

## License

Apache 2.0 — see [LICENSE](http://www.apache.org/licenses/LICENSE-2.0.html).
