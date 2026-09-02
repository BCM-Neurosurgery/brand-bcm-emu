#!/usr/bin/env python
"""
This node automatically screen records the display
and stores the recording in a network share drive.
"""
import os
import re
import sys
import logging
from brand import BRANDNode
import gc
import signal
import subprocess

# need to run this before to get the right display
try:
    with open(os.path.join(os.path.expanduser('~'), '.DISPLAY'), 'r') as f:
        os.environ["DISPLAY"] = f.read().splitlines()[0]
except FileNotFoundError:
    logging.error('No display found, exiting')
    sys.exit(1)


class FFMPEGRecorder(BRANDNode):

    def __init__(self):
        super().__init__()

        self.proc = None

        self.parameters.setdefault('screen_id', None)
        self.parameters.setdefault('screen_width', 1920)
        self.parameters.setdefault('screen_height', 1080)
        self.parameters.setdefault('framerate', 60)
        self.parameters.setdefault('format', 'mkv')
        self.parameters.setdefault('crf', 23)
        self.parameters.setdefault('audio_source', 'pulse')
        self.parameters.setdefault('record_audio', True)
        self.parameters.setdefault('ffmpeg_loglevel', 'error')

        # get parameter values
        self.screen_id = self.parameters['screen_id']
        self.screen_width = self.parameters['screen_width']
        self.screen_height = self.parameters['screen_height']
        self.framerate = self.parameters['framerate']
        self.format = self.parameters['format']
        self.crf = self.parameters['crf']
        self.audio_source = self.parameters['audio_source']
        self.record_audio = self.parameters['record_audio']
        self.ffmpeg_loglevel = self.parameters['ffmpeg_loglevel']

        # get the RDB save path
        rdb_dir = self.r.config_get('dir')['dir']
        # try removing '/mnt' from the path (in case we are at the local
        # version of a network drive)
        rdb_dir_local = rdb_dir.replace('/mnt', '')
        if (not os.path.exists(rdb_dir) and os.path.exists(rdb_dir_local)):
            rdb_dir = rdb_dir_local

        if 'metadata_stream' in self.parameters:
            # read block metadata and wait until the node updates dbfilename
            self.metadata_stream = self.parameters['metadata_stream']
            # get final ID of ms prior to supergraph entry to ensure we xread
            # only this block's metadata
            metadata_id = str(int(self.supergraph_id.split('-')[0]) -
                              1) + '-' + str(0xFFFFFFFFFFFFFFFF)
            metadata = self.r.xread({self.metadata_stream: metadata_id},
                                    block=0)
            metadata = metadata[0][1][-1][1]
            vid_name = metadata[b'block_id'].decode('utf-8')
        else:
            rdb_name = self.r.config_get('dbfilename')['dbfilename']
            vid_name = os.path.splitext(rdb_name)[0]

        # set the video save path
        vid_dir = os.path.abspath(os.path.join(rdb_dir, '..', 'screen_recordings'))
        os.makedirs(vid_dir, exist_ok=True)
        self.vid_path = os.path.join(vid_dir, f'{vid_name}.{self.format}')
        logging.info(f'Recording screen to {self.vid_path}')

        # terminate on SIGINT
        signal.signal(signal.SIGINT, self.terminate)

    def run(self):
        display = os.environ['DISPLAY']
        vid_size = f'{self.screen_width}x{self.screen_height}'

        # Crop to a specific monitor when screen_id is set
        if self.screen_id is not None:
            try:
                out = subprocess.check_output(
                    ["xrandr", "--listmonitors"], text=True, stderr=subprocess.DEVNULL)
            except Exception as e:
                logging.warning(f"xrandr unavailable; skipping screen_id: {e}")
            else:
                screens = [
                    (int(m[3]), int(m[4]), int(m[1]), int(m[2]))
                    for m in re.finditer(r"(\d+)/\d+x(\d+)/\d+\+(\d+)\+(\d+)", out)
                ]
                if 0 <= self.screen_id < len(screens):
                    x, y, w, h = screens[self.screen_id]
                    vid_size = f'{w}x{h}'
                    display = f'{display}+{x},{y}'
                else:
                    logging.warning(
                        f"screen_id={self.screen_id} out of range "
                        f"({len(screens)} detected); recording full display")

        audio_args = (
            f'-f alsa -i {self.audio_source} -acodec aac '
            if self.record_audio else '')
        
        command = (f'ffmpeg -y -loglevel {self.ffmpeg_loglevel} {audio_args}'
                   f'-video_size {vid_size} '
                   f'-framerate {self.framerate} '
                   f'-probesize 100M '
                   f'-f x11grab -i {display} '
                   f'-c:v libx264 -crf {self.crf} -preset ultrafast '
                   f'-vf format=yuv420p '
                   f'{self.vid_path}')
        logging.debug(command)
        
        self.proc = subprocess.Popen(command.split(' '))
        while True:
            signal.pause()

    def terminate(self, sig, frame):
        if self.proc:
            try:
                # ask ffmpeg to stop cleanly so it flushes its encoder and
                # writes the container trailer (moov atom for mp4) -- a bare
                # communicate() never signals ffmpeg at all, so x11grab just
                # keeps recording until the timeout below kills it, which
                # leaves the file with no trailer and unplayable
                self.proc.send_signal(signal.SIGINT)
                self.proc.communicate(timeout=15)
            except subprocess.TimeoutExpired:
                # if it did not exit in time, kill the process
                self.proc.kill()

        logging.info('Screen recording stopped')
        gc.enable()
        return super().terminate(sig, frame)


if __name__ == '__main__':
    gc.disable()
    screen_recorder = FFMPEGRecorder()
    screen_recorder.run()
