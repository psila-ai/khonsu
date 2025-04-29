// This module will contain the distributed commit implementation.

#[cfg(feature = "twopc")]
pub mod twopc;
#[cfg(feature = "twopc")]
pub mod coordinator; // Module for the TwoPhaseCommitCoordinator
#[cfg(feature = "twopc")]
pub mod storage; // Module for OmniPaxos storage backend
