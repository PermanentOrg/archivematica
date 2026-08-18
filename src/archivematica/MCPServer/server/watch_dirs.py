"""
Watched directory handling.

Watched directories are configured in the workflow and start a specific chain
whenever a file or directory is placed in them. They are something we probably
want to remove in future; however, currently they are used extensively in all
workflows, as many chains start the next chain by moving a transfer or SIP t
the appropriate watched directory.
"""

import logging
import os
import sys
import time
import warnings

from django.conf import settings
from inotify_simple import INotify
from inotify_simple import flags

IS_LINUX = sys.platform.startswith("linux")
WATCHED_BASE_DIR = os.path.abspath(settings.WATCH_DIRECTORY)

MOUNTINFO_PATH = "/proc/self/mountinfo"

# Filesystems where inotify cannot observe changes made by other clients.
NETWORK_FILESYSTEM_TYPES = frozenset({"nfs", "nfs4"})

logger = logging.getLogger("archivematica.mcp.server.watchdirs")


def _unescape_mountinfo_field(field):
    """Decode the octal escapes mountinfo uses in path fields."""
    for escape, character in (
        ("\\040", " "),
        ("\\011", "\t"),
        ("\\012", "\n"),
        ("\\134", "\\"),
    ):
        field = field.replace(escape, character)
    return field


def _filesystem_type(path, mountinfo_path=MOUNTINFO_PATH):
    """Return the type of the filesystem that ``path`` lives on.

    Resolves to the most specific mount containing ``path``, so a path under a
    bind- or sub-mount reports that mount rather than its parent. Returns None
    when the type cannot be determined, e.g. on a platform without
    ``/proc/self/mountinfo``.
    """
    try:
        with open(mountinfo_path) as mountinfo:
            lines = mountinfo.readlines()
    except OSError:
        return None

    path = os.path.abspath(path)
    best_mount_point = None
    best_type = None

    for line in lines:
        # Optional fields sit between the mount point and the " - " separator,
        # so the two halves have to be parsed separately.
        before, separator, after = line.partition(" - ")
        if not separator:
            continue
        before_fields = before.split()
        after_fields = after.split()
        if len(before_fields) < 5 or not after_fields:
            continue

        mount_point = _unescape_mountinfo_field(before_fields[4])
        if path != mount_point and not path.startswith(mount_point.rstrip("/") + "/"):
            continue
        if best_mount_point is None or len(mount_point) > len(best_mount_point):
            best_mount_point = mount_point
            best_type = after_fields[0]

    return best_type


def _list_watched_dir_entries(path):
    """
    Return a ``(path, name, is_dir)`` tuple for each entry in ``path``.
    Raises OSError if ``path`` cannot be read.
    """
    with os.scandir(path) as entries:
        return [(entry.path, entry.name, entry.is_dir()) for entry in entries]


def watch_directories_poll(
    watched_dirs, shutdown_event, callback, interval=settings.WATCH_DIRECTORY_INTERVAL
):
    """
    Watch the directories given via poll. This is a very inefficient way to handle
    watches, but it is compatible with all operating systems and filesystems.

    Accepts an iterable of workflow WatchedDir objects, a shutdown event, and a
    callback to be called when content appears in the watched dir.
    """
    # Paths that have already appeared in watch directories, tracked per watched directory.
    known_paths = {}

    while not shutdown_event.is_set():
        for watched_dir in watched_dirs:
            path = os.path.join(WATCHED_BASE_DIR, watched_dir.path.lstrip("/"))

            try:
                entries = _list_watched_dir_entries(path)
            except OSError:
                # On a network filesystem this is expected occasionally, e.g. a
                # stale handle caused by another node renaming an entry out of
                # this directory mid-scan. Keep what we knew and retry.
                logger.warning("Unable to scan watched dir %s", path, exc_info=True)
                continue

            seen_paths = known_paths.get(path, frozenset())
            current_paths = set()

            for item_path, _item_name, is_dir in entries:
                if watched_dir.only_dirs and not is_dir:
                    continue

                # Recorded before the callback runs, so that a callback which
                # raises is not retried on every subsequent pass.
                current_paths.add(item_path)
                if item_path in seen_paths:
                    continue

                try:
                    callback(item_path, watched_dir)
                except Exception:
                    logger.exception("Error starting chain for %s", item_path)

            # Update what we know about from the last pass, so that it doesn't
            # grow endlessly
            known_paths[path] = current_paths

        time.sleep(interval)


def watch_directories_inotify(
    watched_dirs, shutdown_event, callback, interval=settings.WATCH_DIRECTORY_INTERVAL
):
    """
    Watch the directories given via inotify. This is a very efficient way to handle
    watches, however it requires linux and a local filesystem.

    Accepts an iterable of workflow WatchedDir objects, a shutdown event, and a
    callback to be called when content appears in the watched dir.

    Raises RuntimeError if any watched directory is on a network filesystem.
    """
    if not IS_LINUX:
        warnings.warn(
            "inotify may not work as a watched directory method on non-linux systems.",
            RuntimeWarning,
            stacklevel=2,
        )

    for watched_dir in watched_dirs:
        path = os.path.join(WATCHED_BASE_DIR, watched_dir.path.lstrip("/"))
        filesystem_type = _filesystem_type(path)
        if filesystem_type in NETWORK_FILESYSTEM_TYPES:
            raise RuntimeError(
                f'The watched directory "{path}" is on a {filesystem_type} '
                "filesystem. inotify is only told about changes made through "
                "the local kernel, so it never sees a transfer moved into a "
                "watched directory by another node sharing this filesystem, "
                "and those units would stall without any error. Set the "
                '"watch_directory_method" setting to "poll" instead.'
            )

    inotify = INotify()
    watch_flags = flags.CREATE | flags.MOVED_TO
    watches = {}  # descriptor: (path, WatchedDir)

    for watched_dir in watched_dirs:
        path = os.path.join(WATCHED_BASE_DIR, watched_dir.path.lstrip("/"))
        if not os.path.isdir(path):
            raise OSError(f'The path "{path}" is not a directory.')

        descriptor = inotify.add_watch(path, watch_flags)
        watches[descriptor] = (path, watched_dir)

        # If the directory already has something in it, trigger callbacks
        for item in os.scandir(path):
            if watched_dir.only_dirs and not item.is_dir():
                continue
            logger.debug(
                "Found existing data in watched dir %s: %s", watched_dir.path, item.name
            )

            callback(item.path, watched_dir)

    while not shutdown_event.is_set():
        # timeout is in milliseconds
        events = inotify.read(timeout=interval * 1000)
        for event in events:
            path, watched_dir = watches[event.wd]
            logger.debug(
                "Watched dir %s detected activity: %s", watched_dir.path, event.name
            )

            # bitwise check the mask for dirs, if dirs_only is set
            if watched_dir.only_dirs and (flags.ISDIR & event.mask == 0):
                continue

            callback(os.path.join(path, event.name), watched_dir)

    for watch_descriptor in watches.keys():
        inotify.rm_watch(watch_descriptor)

    inotify.close()


def watch_directories(*args, **kwargs):
    method = settings.WATCH_DIRECTORY_METHOD or "poll"

    logger.debug("Starting directory watch (using %s).", method)

    if method == "inotify":
        watch_directories_inotify(*args, **kwargs)
    elif method == "poll":
        watch_directories_poll(*args, **kwargs)
    else:
        raise RuntimeError(f"Unexpected watch method {method}")
