"""Root-only, cold-path CPU partition guard for the formal Consumer service.

Uses a top-level cgroup v2 partition, not merely task affinity. No IRQ changes,
task killing, trading configuration, or service management are performed here.
"""

import argparse
import json
from pathlib import Path

CPUSET = "64-79,160-175"
SLICE = "tradecore.slice"


def parse_cpus(value: str) -> set[int]:
    result: set[int] = set()
    for part in value.strip().split(","):
        if not part:
            continue
        bounds = part.split("-")
        if len(bounds) == 1:
            result.add(int(bounds[0]))
        elif len(bounds) == 2 and 0 <= int(bounds[0]) <= int(bounds[1]) < 4096:
            result.update(range(int(bounds[0]), int(bounds[1]) + 1))
        else:
            raise ValueError("invalid CPU range")
    if any(cpu < 0 or cpu >= 4096 for cpu in result):
        raise ValueError("CPU outside supported range")
    return result


def preflight(root: Path, syscpu: Path) -> Path:
    target = root / SLICE
    wanted = parse_cpus(CPUSET)
    if root.is_symlink() or target.is_symlink() or not target.is_dir():
        raise ValueError("dedicated top-level systemd slice must already exist")
    if not wanted <= parse_cpus((syscpu / "online").read_text()):
        raise ValueError("reserved CPUs must all be online")
    for cpu in wanted:
        siblings = parse_cpus((syscpu / f"cpu{cpu}/topology/thread_siblings_list").read_text())
        if cpu not in siblings or not siblings <= wanted:
            raise ValueError("allocation must contain every SMT sibling")
    if parse_cpus((target / "cpuset.cpus").read_text()) != wanted:
        raise ValueError("slice AllowedCPUs differs from reviewed allocation")
    for name in ("cpuset.cpus.exclusive", "cpuset.cpus.partition"):
        if not (target / name).is_file():
            raise ValueError("kernel lacks required cgroup v2 partition controls")
    # Refuse to silently remove the entire configured pool of another cgroup.
    for path in root.rglob("cpuset.cpus"):
        if target in path.parents:
            continue
        configured = parse_cpus(path.read_text())
        if configured and configured <= wanted:
            raise ValueError(f"foreign cgroup would lose its entire CPU pool: {path.parent}")
    for sibling in root.iterdir():
        if sibling == target or not sibling.is_dir():
            continue
        for name in ("cpuset.cpus.exclusive", "cpuset.cpus.exclusive.effective"):
            path = sibling / name
            if path.exists() and parse_cpus(path.read_text()) & wanted:
                raise ValueError(f"reserved CPUs conflict with another partition: {sibling}")
    return target


def verify(root: Path, target: Path) -> dict[str, object]:
    wanted = parse_cpus(CPUSET)
    state = (target / "cpuset.cpus.partition").read_text().strip()
    effective = (target / "cpuset.cpus.effective").read_text().strip()
    exclusive = (target / "cpuset.cpus.exclusive.effective").read_text().strip()
    if state != "root" or parse_cpus(effective) != wanted or parse_cpus(exclusive) != wanted:
        raise ValueError("partition is not a valid exclusive root with the exact allocation")
    checked = 0
    for sibling in root.iterdir():
        path = sibling / "cpuset.cpus.effective"
        if sibling != target and path.is_file():
            if parse_cpus(path.read_text()) & wanted:
                raise ValueError(f"foreign top-level cgroup still overlaps: {sibling}")
            checked += 1
    return {
        "schema": "tradecore-production-cpu-partition-v1",
        "slice": SLICE,
        "partition": state,
        "effective_cpus": effective,
        "exclusive_cpus": exclusive,
        "foreign_top_level_cgroups_checked": checked,
        "irq_isolation_claimed": False,
        "performance_or_trading_ready_claimed": False,
    }


def apply(root: Path, syscpu: Path) -> dict[str, object]:
    target = preflight(root, syscpu)
    partition = target / "cpuset.cpus.partition"
    exclusive = target / "cpuset.cpus.exclusive"
    old_partition, old_exclusive = partition.read_text().strip(), exclusive.read_text().strip()
    if old_partition not in ("member", "root"):
        raise ValueError("refusing to replace an invalid or externally isolated partition")
    try:
        exclusive.write_text(CPUSET + "\n")
        partition.write_text("root\n")
        return verify(root, target)
    except BaseException:
        # Never leave a newly created half-configured partition after failure.
        partition.write_text(old_partition + "\n")
        exclusive.write_text(old_exclusive + "\n")
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("apply", "check"))
    args = parser.parse_args()
    root = Path("/sys/fs/cgroup")
    syscpu = Path("/sys/devices/system/cpu")
    if args.action == "apply":
        report = apply(root, syscpu)
    else:
        report = verify(root, preflight(root, syscpu))
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
