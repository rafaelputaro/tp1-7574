#!/usr/bin/env python3
"""
Chaos Monkey for TP1-7574 - Randomly kills Docker containers to test resilience

This script randomly kills containers of specified types to test how the system
handles node failures in a distributed environment. It's particularly useful for
testing resilience of sharded architectures.

Usage:
  python chaos_monkey.py [options]

Options:
  --node-type=TYPE    Node type to kill: filter, joiner, aggregator, report, controller, resilience_manager, or any [default: any]
  --kill-count=N      Number of containers to kill [default: 1]
  --interval=N        Seconds to wait between kill rounds [default: 60]
  --rounds=N          Number of kill rounds to perform [default: 5]
  --dry-run           Show what would be killed without actually killing anything
"""

import argparse
import random
import subprocess
import time
import sys


def get_containers_by_type(node_type):
    """
    Get list of container IDs matching the specified node type
    """
    try:
        # Execute docker ps to get running containers
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}},{{.ID}}"],
            capture_output=True,
            text=True,
            check=True
        )

        containers = {}
        for line in result.stdout.strip().split('\n'):
            if not line:
                continue

            parts = line.split(',')
            if len(parts) != 2:
                continue

            name, container_id = parts

            # Filter containers based on node type
            if node_type == "any":
                containers[name] = container_id
            elif node_type == "filter" and "filter" in name:
                containers[name] = container_id
            elif node_type == "joiner" and "joiner" in name:
                containers[name] = container_id
            elif node_type == "aggregator" and "aggregator" in name:
                containers[name] = container_id
            elif node_type == "aggregator_movies" and "aggregator_movies" in name:
                containers[name] = container_id
            elif node_type == "aggregator_top_5" and "aggregator_top_5" in name:
                containers[name] = container_id
            elif node_type == "report" and name == "report":
                containers[name] = container_id
            elif node_type == "controller" and name == "controller":
                containers[name] = container_id
            elif node_type == "resilience_manager" and "resilience_manager" in name:
                containers[name] = container_id

        return containers
    except subprocess.CalledProcessError as e:
        print(f"Error getting container list: {e}")
        sys.exit(1)


def kill_random_containers(containers, count, dry_run=False):
    """
    Kill a random selection of containers
    """
    if not containers:
        print("No matching containers found to kill")
        return []

    # Select random containers to kill, up to the count or available containers
    kill_count = min(count, len(containers))
    targets = random.sample(list(containers.items()), kill_count)

    killed = []
    for name, container_id in targets:
        if dry_run:
            print(f"[DRY RUN] Would kill container: {name} ({container_id})")
            killed.append(name)
        else:
            try:
                print(f"Killing container: {name} ({container_id})")
                subprocess.run(["docker", "kill", container_id], check=True)
                killed.append(name)
            except subprocess.CalledProcessError as e:
                print(f"Failed to kill container {name}: {e}")

    return killed


def main():
    parser = argparse.ArgumentParser(description="Chaos Monkey - Randomly kill Docker containers")
    parser.add_argument("--node-type", default="any",
                        choices=["filter", "joiner", "aggregator", "aggregator_movies", "aggregator_top_5", "report", "controller", "resilience_manager", "any"],
                        help="Type of node to kill (filter, joiner, aggregator, report, controller, resilience_manager, any)")
    parser.add_argument("--kill-count", type=int, default=1,
                        help="Number of containers to kill per round")
    parser.add_argument("--interval", type=int, default=60,
                        help="Seconds to wait between rounds")
    parser.add_argument("--rounds", type=int, default=1,
                        help="Number of rounds to perform")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be killed without actually killing anything")

    args = parser.parse_args()

    print("\n=== TP1-7574 Chaos Monkey ===")
    print(f"Target node type: {args.node_type}")
    print(f"Kill count per round: {args.kill_count}")
    print(f"Interval between rounds: {args.interval} seconds")
    print(f"Number of rounds: {args.rounds}")
    if args.dry_run:
        print("Mode: DRY RUN (no containers will be killed)")
    print("\nStarting chaos sequence...\n")

    round_num = 1
    total_killed = []

    try:
        while round_num <= args.rounds:
            print(f"\n--- Round {round_num}/{args.rounds} ---")

            # Get eligible containers
            containers = get_containers_by_type(args.node_type)

            if not containers:
                print(f"No matching containers of type '{args.node_type}' found. Waiting {args.interval} seconds...")
                time.sleep(args.interval)
                continue

            print(f"Found {len(containers)} eligible containers")

            # Kill containers
            killed = kill_random_containers(containers, args.kill_count, args.dry_run)
            total_killed.extend(killed)

            if killed:
                print(f"Killed {len(killed)} containers")

            if round_num < args.rounds:
                print(f"Waiting {args.interval} seconds until next round...")
                time.sleep(args.interval)

            round_num += 1

    except KeyboardInterrupt:
        print("\n\nChaos Monkey interrupted by user.")

    print("\n=== Chaos Monkey Summary ===")
    print(f"Total containers killed: {len(total_killed)}")
    if total_killed:
        print("Killed containers:")
        for name in total_killed:
            print(f"- {name}")


if __name__ == "__main__":
    main()
