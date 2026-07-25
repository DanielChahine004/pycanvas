"""Child processes that cannot outlive this one (danvasd, reload workers).

A spawned broker must die with the process that launched it — including
hard deaths where no Python cleanup runs: an IDE's stop button is a
``TerminateProcess`` on Windows (not a Ctrl+C), and a ``kill -9`` skips
every handler. Otherwise a stray danvasd silently keeps the port and the
next run greets a stale canvas. :func:`spawn_owned` layers three
guarantees onto ``subprocess.Popen``:

- **atexit** (all platforms): best-effort terminate on any interpreter
  exit — Ctrl+C anywhere in user code, ``sys.exit``, uncaught exceptions,
  a ``block=False`` script falling off the end.
- **Windows**: the child is assigned to a Job Object with
  ``JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE``. The job handle lives exactly as
  long as this process, so when this process dies — *however* it dies —
  the kernel closes the handle and kills the child.
- **Linux**: ``PR_SET_PDEATHSIG`` has the kernel deliver SIGTERM to the
  child when the parent dies, the same guarantee by other means.

macOS has no parent-death signal, so a SIGKILLed parent can still leak
the child there; every softer exit is covered by atexit.
"""

import atexit
import os
import signal
import subprocess
import sys


def spawn_owned(cmd, env=None, **popen_kwargs):
    """``subprocess.Popen(cmd, env=env)`` for a child that must die with us.

    Extra ``popen_kwargs`` pass through (e.g. ``stderr=PIPE`` so a spawner
    can diagnose a startup failure from the child's own words)."""
    kwargs = dict(popen_kwargs)
    if sys.platform.startswith("linux"):
        import ctypes

        libc = ctypes.CDLL(None, use_errno=True)
        PR_SET_PDEATHSIG = 1

        def _pdeathsig():
            libc.prctl(PR_SET_PDEATHSIG, signal.SIGTERM)

        kwargs["preexec_fn"] = _pdeathsig
    proc = subprocess.Popen(cmd, env=env, **kwargs)
    if os.name == "nt":
        try:
            # Keep the job handle referenced for the child's lifetime: the
            # kill fires when the handle CLOSES, which must mean "we died",
            # not "the GC ran".
            proc._danvas_job = _kill_on_close_job(proc)
        except Exception:
            proc._danvas_job = None   # job objects unavailable — atexit still covers

    def _reap():
        if proc.poll() is not None:
            return
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

    atexit.register(_reap)
    return proc


def _kill_on_close_job(proc):
    """Assign ``proc`` to a new kill-on-close Job Object; returns its handle."""
    import ctypes
    from ctypes import wintypes

    class _IO_COUNTERS(ctypes.Structure):
        _fields_ = [(n, ctypes.c_ulonglong) for n in (
            "ReadOperationCount", "WriteOperationCount",
            "OtherOperationCount", "ReadTransferCount",
            "WriteTransferCount", "OtherTransferCount")]

    class _BASIC_LIMITS(ctypes.Structure):
        _fields_ = [("PerProcessUserTimeLimit", wintypes.LARGE_INTEGER),
                    ("PerJobUserTimeLimit", wintypes.LARGE_INTEGER),
                    ("LimitFlags", wintypes.DWORD),
                    ("MinimumWorkingSetSize", ctypes.c_size_t),
                    ("MaximumWorkingSetSize", ctypes.c_size_t),
                    ("ActiveProcessLimit", wintypes.DWORD),
                    ("Affinity", ctypes.c_size_t),
                    ("PriorityClass", wintypes.DWORD),
                    ("SchedulingClass", wintypes.DWORD)]

    class _EXTENDED_LIMITS(ctypes.Structure):
        _fields_ = [("BasicLimitInformation", _BASIC_LIMITS),
                    ("IoInfo", _IO_COUNTERS),
                    ("ProcessMemoryLimit", ctypes.c_size_t),
                    ("JobMemoryLimit", ctypes.c_size_t),
                    ("PeakProcessMemoryUsed", ctypes.c_size_t),
                    ("PeakJobMemoryUsed", ctypes.c_size_t)]

    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000
    JobObjectExtendedLimitInformation = 9

    k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    job = k32.CreateJobObjectW(None, None)
    if not job:
        raise ctypes.WinError(ctypes.get_last_error())
    info = _EXTENDED_LIMITS()
    info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    if not k32.SetInformationJobObject(
            job, JobObjectExtendedLimitInformation,
            ctypes.byref(info), ctypes.sizeof(info)):
        err = ctypes.get_last_error()
        k32.CloseHandle(job)
        raise ctypes.WinError(err)
    if not k32.AssignProcessToJobObject(job, int(proc._handle)):
        err = ctypes.get_last_error()
        k32.CloseHandle(job)
        raise ctypes.WinError(err)
    return job
