#!/usr/bin/env python
# mediaPlayerTask.py
#
# Node 3 for BCM's graph: the task controller. Unlike the reused
# signal-processing nodes elsewhere in this graph, this node never touches
# neural data -- its only job is to load a video/audio file, drive an mpv
# subprocess through it, publish task_state/trial_info to Redis so the rest
# of the graph (and any NWB/derivative export) can align on playback
# timing, and auto-stop the graph when playback ends or is stopped early.
#
# task_state/trial_info schema, nsp timestamp embedding, and auto_stop_*
# handling are copied from fingercomplexTask_commandLine.py (brand-rice) --
# same field names/behavior, so downstream sync/export code doesn't need to
# special-case which task-controller node produced a given graph's data.

import gc
import glob
import json
import logging
import os
import select
import signal
import socket
import subprocess
import sys
import time

import numpy as np
from brand import BRANDNode


class MediaPlayerTask(BRANDNode):

    def __init__(self):
        super().__init__()
        self.parameter_initialization()
        self.mpv_proc = None
        self.ipc_sock = None
        self._ipc_buffer = b''
        signal.signal(signal.SIGINT, self.terminate)

    def parameter_initialization(self):
        self.media_folder_path = self.parameters['media_folder_path']
        self.media_number = str(self.parameters['media_number'])
        self.media_file_path = self.resolve_media_file_path(
            self.media_folder_path, self.media_number)

        self.player_cmd = self.parameters.get('player_cmd', 'mpv')
        self.player_extra_args = self.parameters.get('player_extra_args', [])
        self.ipc_socket_path = self.parameters.get(
            'ipc_socket_path', f'/tmp/{self.NAME}_mpv_ipc.sock')
        self.progress_update_interval_s = float(
            self.parameters.get('progress_update_interval_s', 1.0))

        self.task_state_output_stream = self.parameters.get(
            'task_state_output_stream', 'task_state')
        self.trial_info_output_stream = self.parameters.get(
            'trial_info_output_stream', 'trial_info')
        self.sync_key = self.parameters.get('sync_key', 'sync')
        self.time_key = self.parameters.get('time_key', 'ts')

        # NSP timestamp embedding -- identical to
        # fingercomplexTask_commandLine.py's get_recent_nsp_timestamp, kept
        # under the same parameter names for consistency across the graph.
        self.include_nsp_neural_timestamp = bool(
            self.parameters.get('include_nsp_neural_timestamp', False))
        self.nsp_neural_stream = self.parameters.get(
            'nsp_neural_stream', 'binned_spikes')
        self.include_nsp_analog_timestamp = bool(
            self.parameters.get('include_nsp_analog_timestamp', False))
        self.nsp_analog_stream = self.parameters.get(
            'nsp_analog_stream', 'nsp_neural')
        self.warned_no_analog_stream = False

        self.auto_stop_graph = bool(
            self.parameters.get('auto_stop_graph', True))
        self.auto_stop_do_save = bool(
            self.parameters.get('auto_stop_do_save', True))
        self.auto_stop_do_derivatives = bool(
            self.parameters.get('auto_stop_do_derivatives', True))
        self.block_end_sleep_time_s = float(
            self.parameters.get('block_end_sleep_time_s', 3))

        self.control_nsp_recording = bool(
            self.parameters.get('control_nsp_recording', False))

    @staticmethod
    def resolve_media_file_path(media_folder_path, media_number):
        # Files in media_folder_path are named like '01_...', '02_...' --
        # media_number ('01', '02', ...) picks the file whose name starts
        # with that exact prefix (followed by '_'), so '1' alone won't
        # ambiguously match '01' and '10'/'11' etc.
        pattern = os.path.join(media_folder_path, f'{media_number}_*')
        matches = sorted(glob.glob(pattern))
        if not matches:
            raise FileNotFoundError(
                f'No file matching media_number {media_number!r} found in '
                f'media_folder_path: {media_folder_path}')
        if len(matches) > 1:
            raise RuntimeError(
                f'Multiple files match media_number {media_number!r} in '
                f'{media_folder_path}: {matches}')
        return matches[0]

    # -----------------------------------------------------------
    # NSP timestamp / task_state helpers (mirrors
    # fingercomplexTask_commandLine.py so both task controllers write
    # task_state/trial_info in the same shape)
    # -----------------------------------------------------------

    def get_recent_nsp_timestamp(self):
        recent_nsp_neural_timestamp = 1
        if self.include_nsp_neural_timestamp:
            entry = self.r.xrevrange(self.nsp_neural_stream, count=1)
            if entry:
                recent_nsp_neural_timestamp = np.frombuffer(
                    entry[0][1][b'timestamps'], dtype=np.uint64)[-1]

        recent_nsp_analog_timestamp = 1
        if self.include_nsp_analog_timestamp:
            entry = self.r.xrevrange(self.nsp_analog_stream, count=1)
            if entry:
                recent_nsp_analog_timestamp = np.frombuffer(
                    entry[0][1][b'timestamps'], dtype=np.uint64)[-1]
            elif not self.warned_no_analog_stream:
                logging.warning('No analog stream found in redis.')
                self.warned_no_analog_stream = True

        return recent_nsp_neural_timestamp, recent_nsp_analog_timestamp

    def get_current_redis_time_ms(self):
        t = self.r.time()
        return int(t[0] * 1000 + t[1] / 1000)

    def write_task_state(self, state, extra=None):
        nsp_neural_time, nsp_analog_time = self.get_recent_nsp_timestamp()
        entry = {
            'state': state,
            'redis_timestamp': np.uint64(self.get_current_redis_time_ms()).tobytes(),
            'nsp_neural_timestamp': np.uint64(nsp_neural_time).tobytes(),
            'nsp_analog_timestamp': np.uint64(nsp_analog_time).tobytes(),
            self.sync_key: 0,
            self.time_key: np.uint64(time.monotonic_ns()).tobytes(),
        }
        if extra:
            entry.update(extra)
        self.r.xadd(self.task_state_output_stream, entry)

    # -----------------------------------------------------------
    # mpv process + IPC
    # -----------------------------------------------------------

    def get_player_env(self):
        # supervisor/booter run nodes via `sudo -E`, so this process (and
        # mpv, its child) is root. Root's own runtime dir (/run/user/0) has
        # no PulseAudio session, and root can't auto-discover the real
        # user's session because libpulse's discovery refuses to use a
        # XDG_RUNTIME_DIR it doesn't own. Root *can* reach that session's
        # socket/cookie directly (DAC override) if we skip auto-discovery
        # and hand mpv the explicit paths -- same idea as the ~/.DISPLAY
        # bridge used for X11 elsewhere in this repo. sudo always sets
        # SUDO_UID for the invoking user regardless of -E, so use that
        # rather than trusting XDG_RUNTIME_DIR/HOME to have survived the
        # sudo -E chain intact.
        sudo_uid = os.environ.get('SUDO_UID')
        if not sudo_uid:
            return None
        env = os.environ.copy()
        env.setdefault('PULSE_SERVER', f'unix:/run/user/{sudo_uid}/pulse/native')
        env.setdefault('PULSE_COOKIE', os.path.expanduser('~/.config/pulse/cookie'))
        return env

    def start_player(self):
        # --input-ipc-server gives a JSON-over-unix-socket event feed --
        # this is what lets wait_for_playback_end() tell "played to
        # completion" (end-file reason=eof) apart from "operator closed
        # the window" (reason=quit/stop); a bare subprocess exit code can't
        # make that distinction. --idle=no makes the mpv process itself
        # exit once the file ends, instead of sitting at an idle prompt.
        if os.path.exists(self.ipc_socket_path):
            os.remove(self.ipc_socket_path)

        cmd = [self.player_cmd,
               f'--input-ipc-server={self.ipc_socket_path}',
               '--idle=no',
               '--really-quiet',
               # mpv only opens a window on its own for files with a video
               # track; audio-only files (most of this graph's media) would
               # otherwise play with no visible window at all, leaving the
               # operator no obvious way to stop a trial early. --force-window
               # gives every file -- audio or video -- a window they can
               # close (reason=quit) or press 'q'/'s' in (reason=quit/stop)
               # to end the trial early.
               '--force-window=yes',
               *self.player_extra_args,
               self.media_file_path]
        logging.info(f'Starting player: {" ".join(cmd)}')
        self.mpv_proc = subprocess.Popen(cmd, env=self.get_player_env())

        # mpv creates the socket shortly after startup, not instantly --
        # poll for it rather than guessing a fixed sleep.
        deadline = time.monotonic() + 10.0
        while not os.path.exists(self.ipc_socket_path):
            if self.mpv_proc.poll() is not None:
                raise RuntimeError(
                    f'{self.player_cmd} exited immediately (code '
                    f'{self.mpv_proc.returncode}) before creating its IPC '
                    f'socket -- check media_file_path/player_cmd.')
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f'{self.player_cmd} never created its IPC socket at '
                    f'{self.ipc_socket_path} within 10s.')
            time.sleep(0.05)

        self.ipc_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.ipc_sock.connect(self.ipc_socket_path)
        self.ipc_sock.setblocking(False)

        # subscribe to playback-position updates for progress task_state entries
        self._ipc_send({'command': ['observe_property', 1, 'time-pos']})

    def _ipc_send(self, obj):
        try:
            self.ipc_sock.sendall((json.dumps(obj) + '\n').encode())
        except OSError as e:
            logging.warning(f'Failed to send mpv IPC command: {e}')

    def _read_ipc_events(self, timeout_s):
        # mpv's IPC protocol is newline-delimited JSON; a partial line at
        # the end of a read is buffered and completed on a later call.
        events = []
        ready, _, _ = select.select([self.ipc_sock], [], [], timeout_s)
        if not ready:
            return events
        try:
            chunk = self.ipc_sock.recv(65536)
        except OSError:
            return events
        if not chunk:
            return events
        self._ipc_buffer += chunk
        lines = self._ipc_buffer.split(b'\n')
        self._ipc_buffer = lines[-1]
        for line in lines[:-1]:
            if not line.strip():
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                logging.debug(f'Could not parse mpv IPC line: {line!r}')
        return events

    def wait_for_playback_end(self):
        # Drives the mpv IPC event loop until playback ends, writing
        # periodic progress task_state entries in between. Returns the end
        # reason mpv reports ('eof', 'stop', 'quit', 'error'), or
        # 'process-exited' if the process died without ever sending an
        # end-file event (e.g. someone kill -9'd it).
        last_progress_write = 0.0
        while True:
            events = self._read_ipc_events(timeout_s=0.2)
            for ev in events:
                if ev.get('event') == 'end-file':
                    return ev.get('reason', 'unknown')
                if (ev.get('event') == 'property-change'
                        and ev.get('name') == 'time-pos'
                        and ev.get('data') is not None):
                    now = time.monotonic()
                    if now - last_progress_write >= self.progress_update_interval_s:
                        self.write_task_state('playing', {'time_pos': np.float64(ev['data']).tobytes()})
                        last_progress_write = now

            if self.mpv_proc.poll() is not None:
                return 'process-exited'

    def stop_player(self):
        if self.ipc_sock is not None:
            try:
                self.ipc_sock.close()
            except OSError:
                pass
            self.ipc_sock = None
        if self.mpv_proc is not None and self.mpv_proc.poll() is None:
            self.mpv_proc.terminate()
            try:
                self.mpv_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.mpv_proc.kill()
        if os.path.exists(self.ipc_socket_path):
            os.remove(self.ipc_socket_path)

    # -----------------------------------------------------------
    # main lifecycle
    # -----------------------------------------------------------

    def run(self):
        if self.control_nsp_recording:
            logging.info('Sleeping for a few seconds to let NSP recording start.')
            time.sleep(3)

        trial_start_redis_time = self.get_current_redis_time_ms()
        trial_start_nsp_neural_time, trial_start_nsp_analog_time = \
            self.get_recent_nsp_timestamp()
        self.write_task_state('start_time', {'media_file_path': self.media_file_path})

        self.start_player()
        end_reason = self.wait_for_playback_end()
        self.stop_player()

        trial_end_redis_time = self.get_current_redis_time_ms()
        trial_end_nsp_neural_time, trial_end_nsp_analog_time = \
            self.get_recent_nsp_timestamp()
        finished_naturally = (end_reason == 'eof')
        self.write_task_state('return_time', {'end_reason': end_reason})

        self.r.xadd(self.trial_info_output_stream, {
            'media_file_path': self.media_file_path,
            'trial_start_redis_time': np.uint64(trial_start_redis_time).tobytes(),
            'trial_start_nsp_neural_time': np.uint64(trial_start_nsp_neural_time).tobytes(),
            'trial_start_nsp_analog_time': np.uint64(trial_start_nsp_analog_time).tobytes(),
            'trial_end_redis_time': np.uint64(trial_end_redis_time).tobytes(),
            'trial_end_nsp_neural_time': np.uint64(trial_end_nsp_neural_time).tobytes(),
            'trial_end_nsp_analog_time': np.uint64(trial_end_nsp_analog_time).tobytes(),
            'end_reason': end_reason,
            'finished_naturally': int(finished_naturally),
            self.sync_key: 0,
            self.time_key: np.uint64(time.monotonic_ns()).tobytes(),
        })

        if self.control_nsp_recording:
            # send command to stop central file recording, same as
            # fingercomplexTask_commandLine.py
            self.r.xadd('supervisor_ipstream', {
                'commands': 'updateParameters',
                'centrals_interface': json.dumps({'is_stop_recording_requested': True}),
            })

        logging.info(f'Playback ended (reason={end_reason}). '
                     f'Sending block end signal in {self.block_end_sleep_time_s}s.')
        time.sleep(self.block_end_sleep_time_s)

        if self.auto_stop_graph:
            logging.info('Automatically stopping the graph...')
            self.r.xadd('supervisor_ipstream', {
                'commands': 'stopGraph',
                'do_save': int(self.auto_stop_do_save),
                'do_derivatives': int(self.auto_stop_do_derivatives),
            })

    def terminate(self, sig, frame):
        logging.info('SIGINT received, stopping player and exiting')
        self.stop_player()
        gc.collect()
        sys.exit(0)


if __name__ == '__main__':
    gc.disable()
    node = MediaPlayerTask()
    node.run()
    gc.collect()
