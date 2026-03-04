#!/usr/bin/env python3
"""
Scheduled Force Controller — executes timed force overrides on EBO signals.

Foundation for automated flexibility control: schedules a sequence of force
commands with timed auto-release, allowing programmatic ramp-down/ramp-up
of building heating.

Usage:
    # Dry-run (default): prints what would happen
    python schedule_force.py \
      --building-id HK_Kattegatt_20942 \
      --signal vs1_curve_parallel_shift \
      --schedule "08:30=-0.5, 08:35=-1.0, 08:40=-0.5, 08:45=0" \
      --force-duration 3600

    # Live: actually sends forces to EBO
    python schedule_force.py \
      --building-id HK_Kattegatt_20942 \
      --signal vs1_curve_parallel_shift \
      --schedule "08:30=-0.5, 08:35=-1.0, 08:40=-0.5, 08:45=0" \
      --force-duration 3600 \
      --live
"""

import argparse
import sys
import time
from datetime import datetime, timedelta

from zoneinfo import ZoneInfo

from control_EBO import EBOController

SWEDISH_TZ = ZoneInfo('Europe/Stockholm')


def parse_schedule(schedule_str):
    """Parse schedule string into list of (time_str, value) tuples.

    Format: "HH:MM=value, HH:MM=value, ..."

    Returns:
        list of (str, float) tuples — time string and value
    """
    entries = []
    for part in schedule_str.split(','):
        part = part.strip()
        if not part:
            continue
        if '=' not in part:
            raise ValueError(f"Invalid schedule entry '{part}' — expected HH:MM=value")
        time_str, val_str = part.split('=', 1)
        time_str = time_str.strip()
        val_str = val_str.strip()

        # Validate time format
        try:
            datetime.strptime(time_str, '%H:%M')
        except ValueError:
            raise ValueError(f"Invalid time '{time_str}' — expected HH:MM")

        try:
            value = float(val_str)
        except ValueError:
            raise ValueError(f"Invalid value '{val_str}' — expected a number")

        entries.append((time_str, value))

    if not entries:
        raise ValueError("Empty schedule")

    return entries


def resolve_schedule_times(entries):
    """Convert (time_str, value) tuples to (datetime, value) tuples using today's date.

    All times are in Swedish timezone. Times that have already passed raise an error.

    Returns:
        list of (datetime, float) tuples sorted by time
    """
    now = datetime.now(SWEDISH_TZ)
    today = now.date()
    schedule = []

    for time_str, value in entries:
        hour, minute = map(int, time_str.split(':'))
        dt = datetime(today.year, today.month, today.day, hour, minute, tzinfo=SWEDISH_TZ)
        schedule.append((dt, value))

    # Sort by time
    schedule.sort(key=lambda x: x[0])

    # Check all times are in the future
    past = [(dt, v) for dt, v in schedule if dt <= now]
    if past:
        past_strs = [f"{dt.strftime('%H:%M')}={v}" for dt, v in past]
        raise ValueError(f"Schedule times already passed: {', '.join(past_strs)} (now: {now.strftime('%H:%M:%S')})")

    return schedule


def format_countdown(seconds):
    """Format seconds as human-readable countdown."""
    if seconds <= 0:
        return "now"
    h, rem = divmod(int(seconds), 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h}h {m}m {s}s"
    if m > 0:
        return f"{m}m {s}s"
    return f"{s}s"


def print_schedule_summary(schedule, signal_name, building_id, force_duration, dry_run):
    """Print a formatted schedule summary."""
    mode = "DRY RUN" if dry_run else "LIVE"
    print(f"\n{'='*60}")
    print(f"  Scheduled Force — {mode}")
    print(f"{'='*60}")
    print(f"  Building:  {building_id}")
    print(f"  Signal:    {signal_name}")
    print(f"  Duration:  {force_duration}s per force (safety timeout)")
    print(f"  Events:    {len(schedule)}")
    print()
    print(f"  {'#':>3}  {'Time':>7}  {'Value':>8}  {'Action':<20}")
    print(f"  {'─'*3}  {'─'*7}  {'─'*8}  {'─'*20}")

    for i, (dt, value) in enumerate(schedule, 1):
        if value == 0.0:
            action = "Unforce (release)"
        else:
            action = f"Force to {value}"
        print(f"  {i:>3}  {dt.strftime('%H:%M'):>7}  {value:>8.1f}  {action:<20}")

    print(f"\n{'='*60}\n")


def wait_until(target_dt, api=None):
    """Wait until the target datetime, showing countdown updates.

    Sends keepalive pings to the EBO API every 30s to prevent session timeout.
    """
    last_keepalive = time.monotonic()
    while True:
        now = datetime.now(SWEDISH_TZ)
        remaining = (target_dt - now).total_seconds()

        if remaining <= 0:
            return

        # Keepalive ping every 30s to prevent EBO session timeout
        if api and (time.monotonic() - last_keepalive) >= 30:
            try:
                api.client_refresh(bookmark=0)
            except Exception:
                pass  # Best-effort keepalive
            last_keepalive = time.monotonic()

        # Update countdown display
        countdown = format_countdown(remaining)
        print(f"\r  Waiting... {countdown} until {target_dt.strftime('%H:%M')}  ", end='', flush=True)

        # Sleep interval: shorter as we approach the target
        if remaining > 60:
            time.sleep(10)
        elif remaining > 5:
            time.sleep(1)
        else:
            time.sleep(0.2)


def execute_schedule(ctrl, signal_name, schedule, force_duration):
    """Execute the force schedule, waiting for each event time.

    Args:
        ctrl: EBOController instance (connected)
        signal_name: signal field name to force
        schedule: list of (datetime, float) tuples
        force_duration: seconds for each force timeout
    """
    results = []

    # Read initial value
    initial = ctrl.read_value(signal_name)
    print(f"Initial value: {initial.get('value')} {initial.get('unit', '')}")
    print()

    # Get API reference for keepalive during waits
    api = getattr(ctrl, 'api', None)

    for i, (dt, value) in enumerate(schedule, 1):
        # Wait for scheduled time (with keepalive pings)
        wait_until(dt, api=api)
        print(f"\r  [{dt.strftime('%H:%M:%S')}] Event {i}/{len(schedule)}" + " " * 30)

        now = datetime.now(SWEDISH_TZ)

        if value == 0.0:
            # Unforce — release back to automatic
            print(f"  Action: Unforce (release to automatic)")
            result = ctrl.unforce_signal(signal_name)
        else:
            # Force to value with timeout
            print(f"  Action: Force to {value} (timeout: {force_duration}s)")
            result = ctrl.force_and_verify(signal_name, value, duration_seconds=force_duration)

        success = result.get('success', False)
        dry_run = result.get('dry_run', False)

        if dry_run:
            status = "DRY_RUN"
        elif success:
            status = "OK"
        else:
            status = f"FAILED: {result.get('error', 'unknown')}"

        results.append({
            'time': dt,
            'value': value,
            'status': status,
            'result': result,
        })

        print(f"  Result: {status}")
        print()

    # Print summary
    print(f"\n{'='*60}")
    print(f"  Schedule Complete — Summary")
    print(f"{'='*60}")
    print(f"  {'#':>3}  {'Time':>7}  {'Value':>8}  {'Status':<30}")
    print(f"  {'─'*3}  {'─'*7}  {'─'*8}  {'─'*30}")
    for i, r in enumerate(results, 1):
        print(f"  {i:>3}  {r['time'].strftime('%H:%M'):>7}  {r['value']:>8.1f}  {r['status']:<30}")
    print()

    # Read final value
    final = ctrl.read_value(signal_name)
    print(f"Final value: {final.get('value')} {final.get('unit', '')}")

    return results


def main():
    parser = argparse.ArgumentParser(
        description='Scheduled Force Controller — execute timed force overrides on EBO signals',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Dry-run test schedule (symmetric ramp):
    python schedule_force.py \\
      --building-id HK_Kattegatt_20942 \\
      --signal vs1_curve_parallel_shift \\
      --schedule "08:30=-0.5, 08:35=-1.0, 08:40=-0.5, 08:45=0" \\
      --force-duration 3600

  Live execution:
    python schedule_force.py \\
      --building-id HK_Kattegatt_20942 \\
      --signal vs1_curve_parallel_shift \\
      --schedule "08:30=-0.5, 08:35=-1.0, 08:40=-0.5, 08:45=0" \\
      --force-duration 3600 \\
      --live

Schedule format:
  Comma-separated entries of HH:MM=value (Swedish time).
  Value 0.0 triggers unforce (release to automatic).
  All times must be in the future.
        """,
    )
    parser.add_argument('--building-id', required=True, help='Building ID (e.g. HK_Kattegatt_20942)')
    parser.add_argument('--signal', required=True, help='Signal field name (e.g. vs1_curve_parallel_shift)')
    parser.add_argument('--schedule', required=True,
                        help='Schedule as "HH:MM=value, HH:MM=value, ..." (Swedish time)')
    parser.add_argument('--force-duration', type=int, default=3600,
                        help='Force timeout in seconds (default: 3600 = 1h)')
    parser.add_argument('--live', action='store_true', help='Actually perform forces (default: dry-run)')
    parser.add_argument('--credential-ref', metavar='REF', help='Override credential ref')
    parser.add_argument('--verify-delay', type=int, default=5,
                        help='Seconds to wait for read-back verification (default: 5)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')

    args = parser.parse_args()

    # Parse and validate schedule
    try:
        entries = parse_schedule(args.schedule)
        schedule = resolve_schedule_times(entries)
    except ValueError as e:
        print(f"Schedule error: {e}")
        sys.exit(1)

    dry_run = not args.live

    # Initialize controller
    try:
        ctrl = EBOController(
            building_id=args.building_id,
            dry_run=dry_run,
            verify_delay=args.verify_delay,
            verbose=args.verbose,
        )
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # Validate signal is writable
    try:
        field, sig = ctrl._resolve_signal(args.signal)
    except ValueError as e:
        print(f"Signal error: {e}")
        sys.exit(1)

    if field not in ctrl.writable:
        print(f"Error: Signal '{field}' is not writable.")
        print(f"Writable signals: {sorted(ctrl.writable)}")
        sys.exit(1)

    # Print schedule
    print_schedule_summary(schedule, field, args.building_id, args.force_duration, dry_run)

    # Connect to EBO
    try:
        ctrl.connect(credential_ref_override=args.credential_ref)
    except Exception as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

    # Execute
    try:
        results = execute_schedule(ctrl, field, schedule, args.force_duration)
    except KeyboardInterrupt:
        print(f"\n\nInterrupted! Attempting to unforce signal...")
        try:
            ctrl.unforce_signal(field)
        except Exception as e:
            print(f"Warning: unforce failed: {e}")
            print(f"Signal may still be forced — check manually.")
        sys.exit(1)

    # Exit with error if any event failed
    failures = [r for r in results if 'FAILED' in r['status']]
    if failures:
        print(f"\n{len(failures)} event(s) failed.")
        sys.exit(1)


if __name__ == '__main__':
    main()
