--------------------------- MODULE MCChotkiSync ---------------------------
(* Concrete constants for TLC model checking of ChotkiSync.               *)
(* The .cfg files in this directory pick between these definitions.       *)
EXTENDS ChotkiSync

\* A three-replica chain a -- b -- c: the smallest topology that
\* exercises broadcast relaying (H/D/V forwarding through b).
MCReplicas   == {"a", "b", "c"}
MCEdgesLine  == {{"a", "b"}, {"b", "c"}}
\* The two end replicas commit one op each; the middle one only relays.
MCBudgetAC   == [r \in MCReplicas |-> IF r = "b" THEN 0 ELSE 1]
\* Only "a" commits (smaller model, used for the bug demos).
MCBudgetA3   == [r \in MCReplicas |-> IF r = "a" THEN 1 ELSE 0]

\* A two-replica model for the ping/pong machinery.
MCReplicas2  == {"a", "b"}
MCEdges2     == {{"a", "b"}}
MCBudgetA2   == [r \in MCReplicas2 |-> IF r = "a" THEN 1 ELSE 0]

MCObjects    == {"o1"}

=============================================================================
