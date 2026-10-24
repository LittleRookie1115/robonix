uint64 ts_ns                 # chronos canonical timestamp (CLOCK_MONOTONIC ns)
PowerState power
ComponentHealth[] components
BodyHealth[] bodies          # zero or more body health entries
string dependency_report_json # Vitals-computed dependency evidence and blocked calls; empty if unavailable
