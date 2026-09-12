# Deprecation policy

A supported pipeline field, connector option, or plugin API version is deprecated only through a
release note and an upgrade-guide entry. The notice states the replacement, the first deprecated
release, and the earliest removal release. Deprecated fields remain accepted for at least one minor
release unless a correctness or security defect requires earlier removal; such removals fail with a
specific migration error rather than silently changing behavior.
