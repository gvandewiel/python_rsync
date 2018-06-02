# Standard library modules.
import sys

# External dependencies.
import coloredlogs
from verboselogs import VerboseLogger

# Modules included in our package.
from rotate_backups import (
    RotateBackups,
    coerce_location,
    coerce_retention_period,
)

# Initialize a logger.
logger = VerboseLogger(__name__)


def start_rotation(daily=7, weekly=4, monthly=4, yearly='always',
                   path='', use_sudo=False, strict=False, dry_run=False,
                   exclude=''):
    """Command line interface for the ``rotate-backups`` program."""
    coloredlogs.install(syslog=True)

    # Command line option defaults.
    rotation_scheme = {}
    kw = dict(include_list=[], exclude_list=[])

    # Internal state.
    selected_locations = []

    # Parse the command line arguments.
    rotation_scheme['daily'] = coerce_retention_period(daily)
    rotation_scheme['weekly'] = coerce_retention_period(weekly)
    rotation_scheme['monthly'] = coerce_retention_period(monthly)
    rotation_scheme['yearly'] = coerce_retention_period(yearly)

    # --relaxed mode (Fuzzy date matching)
    kw['strict'] = strict

    # Exclude pattern
    if exclude:
        kw['exclude_list'].append(exclude)

    # Perform a dry run
    kw['dry_run'] = dry_run
        
    print(kw)
    if path:
        selected_locations.append(coerce_location(path, sudo=use_sudo))
        # selected_locations.extend(coerce_location(path, sudo=use_sudo))
    else:
        # Show the usage message when no directories are given nor configured.
        print("No location(s) to rotate selected.")
        return

    # Rotate the backups in the selected directories.
    program = RotateBackups(rotation_scheme, **kw)
    for location in selected_locations:
        program.rotate_backups(location)
