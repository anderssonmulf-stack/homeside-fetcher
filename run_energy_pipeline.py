#!/usr/bin/env python3
"""
Manual Energy Pipeline Runner

Runs the full energy data pipeline manually for testing:
1. Import energy data from Dropbox
2. Separate heating vs DHW energy
3. Recalibrate k-values

Usage:
    python run_energy_pipeline.py              # Run full pipeline
    python run_energy_pipeline.py --dry-run    # Show what would happen
    python run_energy_pipeline.py --step import      # Only run import
    python run_energy_pipeline.py --step separate    # Only run separation
    python run_energy_pipeline.py --step calibrate   # Only run k-value calibration

Environment variables:
    DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN
    INFLUXDB_URL, INFLUXDB_TOKEN, INFLUXDB_ORG, INFLUXDB_BUCKET
    SEQ_URL, SEQ_API_KEY (optional)
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_import(dry_run: bool = False) -> dict:
    """Run energy import from Dropbox."""
    from energy_importer import EnergyImporter
    from dropbox_client import create_client_from_env

    print("\n" + "="*50)
    print("📥 STEP 1: Import Energy Data from Dropbox")
    print("="*50)

    dropbox_client = create_client_from_env()
    if not dropbox_client:
        print("❌ Dropbox not configured")
        print("   Set DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_REFRESH_TOKEN")
        return {'success': False, 'error': 'Dropbox not configured'}

    importer = EnergyImporter(
        dropbox_client=dropbox_client,
        influx_url=os.getenv('INFLUXDB_URL', 'http://localhost:8086'),
        influx_token=os.getenv('INFLUXDB_TOKEN', ''),
        influx_org=os.getenv('INFLUXDB_ORG', 'homeside'),
        influx_bucket=os.getenv('INFLUXDB_BUCKET', 'heating'),
        profiles_dir='profiles',
        dry_run=dry_run
    )

    try:
        result = importer.run(sync_meters=True)
        importer.close()

        if result.get('records', 0) > 0:
            print(f"\n✓ Imported {result['records']} records from {result['files']} file(s)")
        elif result.get('files', 0) == 0:
            print("\nℹ No energy files to import")
        else:
            print(f"\n⚠ Found {result['files']} file(s) but imported 0 records")
            if result.get('errors'):
                for error in result['errors']:
                    print(f"   - {error}")

        return {'success': True, **result}

    except Exception as e:
        print(f"\n❌ Import failed: {e}")
        return {'success': False, 'error': str(e)}
    finally:
        importer.close()


def run_separation(dry_run: bool = False, hours: int = 48) -> dict:
    """Run energy separation (heating vs DHW)."""
    from energy_separation_service import EnergySeparationService

    print("\n" + "="*50)
    print("⚡ STEP 2: Separate Heating vs DHW Energy")
    print("="*50)

    service = EnergySeparationService(
        influx_url=os.getenv('INFLUXDB_URL', 'http://localhost:8086'),
        influx_token=os.getenv('INFLUXDB_TOKEN', ''),
        influx_org=os.getenv('INFLUXDB_ORG', 'homeside'),
        influx_bucket=os.getenv('INFLUXDB_BUCKET', 'heating'),
        profiles_dir='profiles',
        dry_run=dry_run
    )

    try:
        result = service.run(hours=hours)

        if result.get('records', 0) > 0:
            print(f"\n✓ Separated energy for {result['houses']} house(s), {result['records']} record(s)")
        elif result.get('houses', 0) == 0:
            print("\nℹ No houses with energy separation enabled")
        else:
            print(f"\n⚠ Found {result['houses']} enabled house(s) but no data to separate")

        return {'success': True, **result}

    except Exception as e:
        print(f"\n❌ Separation failed: {e}")
        return {'success': False, 'error': str(e)}
    finally:
        service.close()


def run_calibration(dry_run: bool = False, days: int = 30) -> dict:
    """Run k-value recalibration."""
    from k_recalibrator import KRecalibrator
    from customer_profile import CustomerProfile

    print("\n" + "="*50)
    print("📊 STEP 3: Recalibrate K-Values")
    print("="*50)

    recalibrator = KRecalibrator(
        influx_url=os.getenv('INFLUXDB_URL', 'http://localhost:8086'),
        influx_token=os.getenv('INFLUXDB_TOKEN', ''),
        influx_org=os.getenv('INFLUXDB_ORG', 'homeside'),
        influx_bucket=os.getenv('INFLUXDB_BUCKET', 'heating'),
        dry_run=dry_run
    )

    profiles_dir = 'profiles'
    calibrated = []

    try:
        for filename in os.listdir(profiles_dir):
            if not filename.endswith('.json'):
                continue

            try:
                profile = CustomerProfile.load_by_path(
                    os.path.join(profiles_dir, filename)
                )

                if not profile.energy_separation.enabled:
                    continue

                print(f"\nCalibrating {profile.friendly_name} ({profile.customer_id})...")

                result = recalibrator.recalibrate(
                    profile,
                    days=days,
                    update_profile=not dry_run
                )

                if result:
                    calibrated.append({
                        'house': profile.friendly_name,
                        'house_id': profile.customer_id,
                        'k_value': result.k_value,
                        'confidence': result.confidence,
                        'days_used': result.days_used
                    })
                    print(f"  ✓ k = {result.k_value:.4f} kW/°C ({result.confidence:.0%} confidence, {result.days_used} days)")
                else:
                    print(f"  ⚠ Insufficient data for calibration")

            except Exception as e:
                logger.warning(f"Failed to process {filename}: {e}")

        if calibrated:
            print(f"\n✓ Calibrated {len(calibrated)} house(s)")
        else:
            print("\nℹ No houses ready for calibration")

        return {'success': True, 'houses': len(calibrated), 'results': calibrated}

    except Exception as e:
        print(f"\n❌ Calibration failed: {e}")
        return {'success': False, 'error': str(e)}


def run_full_pipeline(dry_run: bool = False) -> dict:
    """Run the full energy pipeline."""
    from seq_logger import SeqLogger

    print("\n" + "="*60)
    print("🔄 FULL ENERGY PIPELINE")
    print("="*60)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}")

    # Initialize Seq logger
    seq = SeqLogger(
        component='EnergyPipeline',
        friendly_name='EnergyPipeline'
    )

    results = {
        'import': run_import(dry_run),
        'separation': run_separation(dry_run),
        'calibration': run_calibration(dry_run)
    }

    # Summary
    print("\n" + "="*60)
    print("📋 PIPELINE SUMMARY")
    print("="*60)
    print(f"  Import:      {results['import'].get('records', 0)} records " +
          ("✓" if results['import']['success'] else "❌"))
    print(f"  Separation:  {results['separation'].get('records', 0)} records " +
          ("✓" if results['separation']['success'] else "❌"))
    print(f"  Calibration: {results['calibration'].get('houses', 0)} house(s) " +
          ("✓" if results['calibration']['success'] else "❌"))

    all_success = all(r['success'] for r in results.values())
    print(f"\nOverall: {'✓ SUCCESS' if all_success else '⚠ PARTIAL'}")
    print("="*60 + "\n")

    # Log to Seq
    seq.log(
        "Energy pipeline manual run: import={ImportRecords}, separation={SepRecords}, calibration={CalHouses}",
        level='Information' if all_success else 'Warning',
        properties={
            'ImportRecords': results['import'].get('records', 0),
            'ImportFiles': results['import'].get('files', 0),
            'SepRecords': results['separation'].get('records', 0),
            'SepHouses': results['separation'].get('houses', 0),
            'CalHouses': results['calibration'].get('houses', 0),
            'DryRun': dry_run,
            'Success': all_success
        }
    )

    return results


def main():
    parser = argparse.ArgumentParser(
        description='Run energy pipeline manually',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_energy_pipeline.py                    # Full pipeline
  python run_energy_pipeline.py --dry-run          # Test without writing
  python run_energy_pipeline.py --step import      # Only import
  python run_energy_pipeline.py --step separate    # Only separation
  python run_energy_pipeline.py --step calibrate   # Only calibration
        """
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Show what would happen without writing data'
    )
    parser.add_argument(
        '--step', choices=['import', 'separate', 'calibrate'],
        help='Run only a specific step'
    )
    parser.add_argument(
        '--hours', type=int, default=48,
        help='Hours of data for separation (default: 48)'
    )
    parser.add_argument(
        '--days', type=int, default=30,
        help='Days of data for calibration (default: 30)'
    )
    args = parser.parse_args()

    # Check required env vars
    if not os.getenv('INFLUXDB_TOKEN') and not args.dry_run:
        print("ERROR: INFLUXDB_TOKEN not set")
        sys.exit(1)

    if args.step == 'import':
        result = run_import(args.dry_run)
        success = result.get('success', False)
    elif args.step == 'separate':
        result = run_separation(args.dry_run, args.hours)
        success = result.get('success', False)
    elif args.step == 'calibrate':
        result = run_calibration(args.dry_run, args.days)
        success = result.get('success', False)
    else:
        result = run_full_pipeline(args.dry_run)
        # Full pipeline returns dict with multiple results
        success = all(r.get('success', False) for r in result.values())

    # Exit code
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
