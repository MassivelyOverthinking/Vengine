# Windjam - Project Roadmap

This document tracks the architectural, implementation and stabilization phases of the Windjam project. 
Each phase is intentionally scoped, sequential and auditable.

**Core Design Principles**
- Correctness before cleverness
- Explicit invariants
- Lazy, columnar, Arrow-native execution
- Clear separation between planning vs. execution
- Deterministic, reproduceable behavoir

---

## Phase 1 - System Design & Architecture

**State:** ✅ Completed

### Goal
Establish a stable conceptual and architectural foundation for Windjam.

### Design Principles
- Planning is not equal to execution
- Stateless, symmbolic operations
- Schema- and column-first reasoning
- Lazy execution is first class

### Key Outcomes
- Defined core abstractions:
    - Conduit
    - Reader
    - Waypoint
    - Sink
    - Storage
- Establish 2-phase lifecycle:
    - Construction/Build
    - Execution
- Polars LazyFrame + PyArrow exeuction
- Column Dependency Graph modeling.

--- 

## Phase 2 — Class Modeling

**State:** ✅ Completed

### Goal
Define stable, and minimal base class interfaces.

### Design Principles
- Simplistic interfaces
- Immutable configuration
- No hidden runtime state
- Explicit contracts over implicit behavior

### Key Outcomes
- Abstract base classes defined for:
  - BaseReader
  - BaseWaypoint
  - BaseSink
- Common invariants identified:
  - Required columns
  - Produced columns
  - Schema expectations
- Clear separation between:
  - Configuration-time validation
  - Build-time validation
  - Execution-time validation

---

## Phase 3 — Reader Configuration & Integration

**State:** 🟡 Ongoing

### Goal
Implement concrete Reader classes for core data file formats that produce valid Polars LazyFrames without triggering execution/materilization.

### Design Principles
- Zero eager materialization/transformation
- Arrow-native data types
- I/O operations isolated to Reader-classes
- Deterministic schema validation

### Tasks
- [x] Implement `BaseReader` contract
- [ ] Define schema discovery semantics
- [x] Support multiple data sources (e.g. files, in-memory, connectors)
- [x] Validate reader output schema early
- [x] Enforce LazyFrame-only output
- [x] Convert Reards to use customm Schema instead of Polars-native

---

## Phase 4 — Waypoint Configuration & Integration

**State:** ⏳ Pending

### Goal
Implement stateless, symbolic schema operations and validations.

### Design Principles
- Purely logical operations
- No runtime state
- Column-level dependency declaration
- No data materialization

### Tasks
- [ ] Implement `BaseWaypoint` contract
- [ ] Enforce required / produced column declaration by Graph
- [ ] Encode validation semantics symbolically
- [ ] Support schema evolution rules
- [ ] Enable validation fusion opportunities

---

## Phase 5 — Sink Configuration & Integration

**State:** ⏳ Pending

### Goal
Define explicit Sink execution and materialization operations.

### Design Principles
- Sink-driven actions
- Explicit data materialization
- Clear I/O ownership
- No implicit side effects

### Tasks
- [ ] Implement `BaseSink` contract
- [ ] Define materialization triggers
- [ ] Support multiple sink types (e.g. disk, memory, callbacks)
- [ ] Enforce execution determinism
- [ ] Prevent accidental eager execution upstream

--

## Phase 6 — Conduit Orchestration & Integration

**State:** ⏳ Pending

### Goal
Bind Readers, Waypoints, and Sinks into a single immutable execution plan (Conduit).

### Design Principles
- 2-phase lifecycle
- Immutability
- Thread-safe
- Shareable and cacheable
- `get_state` functionality

### Tasks
- [ ] Implement construction-phase mutability
- [ ] Implement `.build()` operations
- [ ] Prevent post-build mutation
- [ ] Validate full pipeline structure
- [ ] Prepare execution plans without running them

---

## Phase 7 — Columnar Dependency Graph Structure

**State:** ⏳ Pending

### Goal
Construct a static Column Dependency Graph (CDG) during build-time.

### Design Principles
- Planning-only data structure
- Column dependencies, not actual values
- No runtime or hot-path usage
- Fail fast on structural errors (`build-phase`)

### Tasks
- [ ] Define CDG node and edge operations
- [ ] Track required / produced columns
- [ ] Enable column pruning
- [ ] Support projection pushdown
- [ ] Validate dependency completeness

---

## Phase 8 — Documentation

**State:** ⏳ Pending

### Goal
Make the system self-explanatory for advanced users and contributors (Docstrings)

### Design Principles
- Explain *why*, not just *what*
- Document invariants and guarantees
- Align docs with actual behavior
- Utilise AI-optimized XDoc standard

### Tasks
- [ ] Class-level docstrings
- [ ] Method-level docstrings
- [ ] Lifecycle documentation
- [ ] Architecture overview
- [ ] Example pipelines

---

## Phase 9 — Review, Optimization & Hardening

**State:** ⏳ Pending

### Goal
Stress-test architectural decisions and remove accidental complexity.

### Design Principles
- Optimize for data scale, not config scale
- Reduce memory pressure
- Avoid Python object churn and materialization
- Preserve determinism
- Reproduce behavoir

### Tasks
- [ ] Review execution plans
- [ ] Validate lazy behavior end-to-end
- [ ] Identify unnecessary column operations
- [ ] Check for hidden hot-paths
- [ ] Simplify abstractions where possible

---

## Phase 10 — Testing & Validation

**State:** ⏳ Pending

### Goal
Prove correctness, determinism, and safety.

### Design Principles
- Structural tests before data tests (Unit)
- Deterministic test inputs
- Fail loudly and early
- Validate `Deadstop` execution

### Tasks
- [ ] Unit tests for base classes
- [ ] Build-time validation tests
- [ ] Determinism tests
- [ ] Comprehensive edge case testing
- [ ] Concurrency checks
