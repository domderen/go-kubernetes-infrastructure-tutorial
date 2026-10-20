"""Process safety checks use only children created by these tests."""
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import unittest

import lab


class ProcessControlsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.original_runtime = lab.RUNTIME
        lab.RUNTIME = Path(self.temporary.name)
        self.children = []

    def tearDown(self):
        for child in self.children:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)
        for name in list(lab.state()):
            lab.stop(name, kill=True)
        lab.RUNTIME = self.original_runtime
        self.temporary.cleanup()

    def child(self):
        command = ["/bin/sleep", "30"]
        child = subprocess.Popen(command, stdin=subprocess.DEVNULL)
        self.children.append(child)
        return child, command

    # Catches accepting the executable alone when a PID belongs to other arguments.
    def test_alive_requires_the_entire_recorded_command(self):
        child, command = self.child()
        self.assertTrue(lab.alive({"pid": child.pid, "command": command}))
        self.assertFalse(lab.alive({"pid": child.pid, "command": command + ["different-state-file"]}))

    # Catches stop() signaling an unrelated invocation of the same executable.
    def test_stop_never_signals_a_mismatched_process(self):
        child, command = self.child()
        lab.save_state({"provider": {"pid": child.pid, "command": command + ["not-this-lab"]}})
        lab.stop("provider", kill=True)
        self.assertIsNone(child.poll())
        self.assertEqual(lab.state(), {})

    # Catches an up/rebuild silently retaining an old executable instance.
    def test_start_reuses_unchanged_process_and_restarts_changed_binary(self):
        command = ["/bin/sleep", "30"]
        first = lab.start("worker", command)
        self.assertEqual(lab.start("worker", command), first)
        old = lab.state()
        old["worker"]["binaryMtime"] = -1
        lab.save_state(old)
        second = lab.start("worker", command)
        self.assertNotEqual(second, first)
        lab.stop("worker", kill=True)


if __name__ == "__main__":
    unittest.main()
