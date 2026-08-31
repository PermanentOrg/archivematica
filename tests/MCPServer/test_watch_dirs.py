from unittest import mock

import pytest

from archivematica.MCPServer.server import watch_dirs
from archivematica.MCPServer.server.watch_dirs import _filesystem_type
from archivematica.MCPServer.server.watch_dirs import _list_watched_dir_entries
from archivematica.MCPServer.server.watch_dirs import watch_directories
from archivematica.MCPServer.server.watch_dirs import watch_directories_inotify
from archivematica.MCPServer.server.watch_dirs import watch_directories_poll


class StubWatchedDir:
    """Stands in for a workflow WatchedDir."""

    def __init__(self, path, only_dirs=False):
        self.path = path
        self.only_dirs = only_dirs


class StubShutdownEvent:
    """Reports "not set" for a fixed number of polls, then "set".

    Lets a test run the poll loop for an exact number of cycles.
    """

    def __init__(self, cycles):
        self.remaining = cycles

    def is_set(self):
        if self.remaining <= 0:
            return True
        self.remaining -= 1
        return False


def run_poll(tmp_path, watched_dirs, callback, cycles=1):
    """Run the poll loop over ``tmp_path`` for ``cycles`` iterations."""
    with (
        mock.patch.object(watch_dirs, "WATCHED_BASE_DIR", str(tmp_path)),
        mock.patch.object(watch_dirs.time, "sleep"),
    ):
        watch_directories_poll(
            watched_dirs, StubShutdownEvent(cycles), callback, interval=0
        )


# _list_watched_dir_entries


def test_list_watched_dir_entries_returns_paths_names_and_dir_flags(tmp_path):
    (tmp_path / "a_file.txt").write_text("hello")
    (tmp_path / "a_dir").mkdir()

    entries = sorted(_list_watched_dir_entries(str(tmp_path)))

    assert entries == sorted(
        [
            (str(tmp_path / "a_file.txt"), "a_file.txt", False),
            (str(tmp_path / "a_dir"), "a_dir", True),
        ]
    )


def test_list_watched_dir_entries_returns_empty_list_for_empty_dir(tmp_path):
    assert _list_watched_dir_entries(str(tmp_path)) == []


def test_list_watched_dir_entries_raises_oserror_for_unreadable_dir(tmp_path):
    with pytest.raises(OSError):
        _list_watched_dir_entries(str(tmp_path / "does_not_exist"))


# watch_directories_poll


def test_poll_calls_callback_for_new_entry(tmp_path):
    (tmp_path / "transfer").mkdir()
    watched_dir = StubWatchedDir("/", only_dirs=True)
    callback = mock.Mock()

    run_poll(tmp_path, [watched_dir], callback, cycles=1)

    callback.assert_called_once_with(str(tmp_path / "transfer"), watched_dir)


def test_poll_does_not_call_callback_twice_for_same_entry(tmp_path):
    (tmp_path / "transfer").mkdir()
    callback = mock.Mock()

    run_poll(tmp_path, [StubWatchedDir("/", only_dirs=True)], callback, cycles=3)

    callback.assert_called_once()


def test_poll_skips_files_when_only_dirs_is_set(tmp_path):
    (tmp_path / "a_file.txt").write_text("hello")
    callback = mock.Mock()

    run_poll(tmp_path, [StubWatchedDir("/", only_dirs=True)], callback, cycles=1)

    callback.assert_not_called()


def test_poll_keeps_running_when_a_directory_cannot_be_scanned(tmp_path):
    """A transient scandir failure must not end the watch loop.

    On EFS an ESTALE from a concurrent rename would otherwise propagate out of
    the watcher thread, silently stopping every watched directory.
    """
    (tmp_path / "transfer").mkdir()
    watched_dir = StubWatchedDir("/", only_dirs=True)
    callback = mock.Mock()
    listings = [
        OSError(116, "Stale file handle"),
        [(str(tmp_path / "transfer"), "transfer", True)],
    ]

    with mock.patch.object(
        watch_dirs, "_list_watched_dir_entries", side_effect=listings
    ):
        run_poll(tmp_path, [watched_dir], callback, cycles=2)

    callback.assert_called_once_with(str(tmp_path / "transfer"), watched_dir)


def test_poll_does_not_refire_known_entries_after_a_scan_failure(tmp_path):
    """A failed scan must not make already-seen entries look new again.

    Dropping them from the known set would start a second chain for a package
    that is already being processed.
    """
    entry = (str(tmp_path / "transfer"), "transfer", True)
    callback = mock.Mock()
    listings = [[entry], OSError(116, "Stale file handle"), [entry]]

    with mock.patch.object(
        watch_dirs, "_list_watched_dir_entries", side_effect=listings
    ):
        run_poll(tmp_path, [StubWatchedDir("/", only_dirs=True)], callback, cycles=3)

    callback.assert_called_once()


def test_poll_keeps_running_when_the_callback_raises(tmp_path):
    (tmp_path / "one").mkdir()
    (tmp_path / "two").mkdir()
    callback = mock.Mock(side_effect=[ValueError("boom"), None])

    run_poll(tmp_path, [StubWatchedDir("/", only_dirs=True)], callback, cycles=1)

    assert callback.call_count == 2


def test_poll_scans_remaining_directories_when_one_fails(tmp_path):
    good = tmp_path / "good"
    good.mkdir()
    (good / "transfer").mkdir()
    callback = mock.Mock()
    watched_dirs = [StubWatchedDir("/missing"), StubWatchedDir("/good", only_dirs=True)]

    run_poll(tmp_path, watched_dirs, callback, cycles=1)

    callback.assert_called_once_with(str(good / "transfer"), watched_dirs[1])


# _filesystem_type


def write_mountinfo(tmp_path, lines):
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text("".join(f"{line}\n" for line in lines))
    return str(mountinfo)


def test_filesystem_type_returns_type_of_longest_matching_mount(tmp_path):
    mountinfo = write_mountinfo(
        tmp_path,
        [
            "23 1 0:1 / / rw,relatime - ext4 /dev/root rw",
            "44 23 0:5 / /var/archivematica/sharedDirectory rw - nfs4 fs.efs:/ rw",
        ],
    )

    assert (
        _filesystem_type(
            "/var/archivematica/sharedDirectory/watchedDirectories",
            mountinfo_path=mountinfo,
        )
        == "nfs4"
    )


def test_filesystem_type_returns_type_of_enclosing_mount_for_unmounted_path(tmp_path):
    mountinfo = write_mountinfo(
        tmp_path, ["23 1 0:1 / / rw,relatime - ext4 /dev/root rw"]
    )

    assert _filesystem_type("/var/archivematica", mountinfo_path=mountinfo) == "ext4"


def test_filesystem_type_ignores_mounts_that_only_share_a_name_prefix(tmp_path):
    mountinfo = write_mountinfo(
        tmp_path,
        [
            "23 1 0:1 / / rw,relatime - ext4 /dev/root rw",
            "44 23 0:5 / /var/archivematica-other rw - nfs4 fs.efs:/ rw",
        ],
    )

    assert _filesystem_type("/var/archivematica", mountinfo_path=mountinfo) == "ext4"


def test_filesystem_type_handles_optional_fields_before_the_separator(tmp_path):
    mountinfo = write_mountinfo(
        tmp_path,
        ["44 23 0:5 / /shared rw shared:2 master:1 - nfs4 fs.efs:/ rw"],
    )

    assert _filesystem_type("/shared", mountinfo_path=mountinfo) == "nfs4"


def test_filesystem_type_returns_none_when_mountinfo_is_unavailable(tmp_path):
    assert _filesystem_type("/shared", mountinfo_path=str(tmp_path / "nope")) is None


# inotify rejection on network filesystems


def test_inotify_raises_when_watched_dir_is_on_a_network_filesystem(tmp_path):
    watched_dirs = [StubWatchedDir("/activeTransfers")]

    with (
        mock.patch.object(watch_dirs, "WATCHED_BASE_DIR", str(tmp_path)),
        mock.patch.object(watch_dirs, "_filesystem_type", return_value="nfs4"),
        pytest.raises(RuntimeError, match="nfs4"),
    ):
        watch_directories_inotify(watched_dirs, StubShutdownEvent(0), mock.Mock())


def test_inotify_error_names_the_poll_alternative(tmp_path):
    with (
        mock.patch.object(watch_dirs, "WATCHED_BASE_DIR", str(tmp_path)),
        mock.patch.object(watch_dirs, "_filesystem_type", return_value="nfs"),
        pytest.raises(RuntimeError, match="poll"),
    ):
        watch_directories_inotify(
            [StubWatchedDir("/activeTransfers")], StubShutdownEvent(0), mock.Mock()
        )


def test_watch_directories_rejects_inotify_on_a_network_filesystem(tmp_path):
    with (
        mock.patch.object(watch_dirs, "WATCHED_BASE_DIR", str(tmp_path)),
        mock.patch.object(watch_dirs, "_filesystem_type", return_value="nfs4"),
        mock.patch.object(watch_dirs.settings, "WATCH_DIRECTORY_METHOD", "inotify"),
        pytest.raises(RuntimeError),
    ):
        watch_directories(
            [StubWatchedDir("/activeTransfers")], StubShutdownEvent(0), mock.Mock()
        )


def test_inotify_is_allowed_on_a_local_filesystem(tmp_path):
    """inotify stays usable for local development on a real local filesystem."""
    (tmp_path / "activeTransfers").mkdir()

    with (
        mock.patch.object(watch_dirs, "WATCHED_BASE_DIR", str(tmp_path)),
        mock.patch.object(watch_dirs, "_filesystem_type", return_value="ext4"),
    ):
        watch_directories_inotify(
            [StubWatchedDir("/activeTransfers")], StubShutdownEvent(0), mock.Mock()
        )
