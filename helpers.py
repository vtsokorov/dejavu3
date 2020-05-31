# -*- coding: utf-8 -*-

import os
import fnmatch
from hashlib import sha1
from itertools import zip_longest
from operator import itemgetter

import wavio
import numpy as np
from pydub import AudioSegment
from pydub.utils import audioop

import matplotlib.mlab as mlab

from scipy.ndimage.filters import maximum_filter
from scipy.ndimage.morphology import generate_binary_structure, \
    iterate_structure, binary_erosion


IDX_FREQ_I = 0
IDX_TIME_J = 1
DEFAULT_FS = 44100
DEFAULT_WINDOW_SIZE = 4096
DEFAULT_OVERLAP_RATIO = 0.5
DEFAULT_FAN_VALUE = 15
DEFAULT_AMP_MIN = 10
PEAK_NEIGHBORHOOD_SIZE = 20
MIN_HASH_TIME_DELTA = 0
MAX_HASH_TIME_DELTA = 200
PEAK_SORT = True
FINGERPRINT_REDUCTION = 20


def unique_hash(filepath, blocksize=2 ** 20):
    s = sha1()
    with open(filepath, "rb") as f:
        while True:
            buf = f.read(blocksize)
            if not buf:
                break
            s.update(buf)
    return s.hexdigest().upper()


def find_files(path, extensions=['wav']):
    extensions = [e.replace(".", "") for e in extensions]
    for dirpath, dirnames, files in os.walk(path):
        for extension in extensions:
            for f in fnmatch.filter(files, "*.{}".format(extension)):
                p = os.path.join(dirpath, f)
                yield (p, extension)


def read(filename, limit=None):
    try:
        audiofile = AudioSegment.from_file(filename)

        if limit:
            audiofile = audiofile[:limit * 1000]

        data = np.fromstring(audiofile._data, np.int16)

        channels = []
        for chn in range(audiofile.channels):
            channels.append(data[chn::audiofile.channels])

        frame_rate = audiofile.frame_rate

    except audioop.error:

        wav = wavio.read(filename)
        audiofile = wav.data

        if limit:
            audiofile = audiofile[:limit * 1000]

        audiofile = audiofile.T
        audiofile = audiofile.astype(np.int16)

        channels = []
        for chn in audiofile:
            channels.append(chn)

        frame_rate = wav.rate

    return channels, frame_rate, unique_hash(filename)


def path_to_record(path):
    return os.path.splitext(os.path.basename(path))[0]


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return (filter(None, values) for values
            in zip_longest(fillvalue=fillvalue, *args))


def fingerprint_worker(filename, limit=None, recodr_name=None):
    try:
        filename, limit = filename
    except ValueError:
        pass

    recordname, extension = os.path.splitext(os.path.basename(filename))
    recodr_name = recodr_name or recordname
    channels, fs, file_hash = read(filename, limit)
    result = set()
    channel_amount = len(channels)

    for channeln, channel in enumerate(channels):
        print(
            'Fingerprinting channel {}/{} for {}'.format(
                channeln + 1,
                channel_amount,
                filename
            )
        )
        hashes = fingerprint(channel, fs=fs)
        print(
            'Finished channel {}/{} for {}'.format(
                channeln + 1,
                channel_amount,
                filename
            )
        )
        result |= set(hashes)

    return recodr_name, result, file_hash


def fingerprint(channel_samples, fs=DEFAULT_FS):
    """
    FFT the channel, log transform output, find local maxima, then return
    locally sensitive hashes.
    """
    # FFT the signal and extract frequency components
    arr2D = mlab.specgram(
        channel_samples,
        NFFT=DEFAULT_WINDOW_SIZE,
        Fs=fs,
        window=mlab.window_hanning,
        noverlap=int(DEFAULT_WINDOW_SIZE * DEFAULT_OVERLAP_RATIO)
    )[0]

    # apply log transform since specgram() returns linear array
    arr2D = 10 * np.log10(arr2D)
    arr2D[arr2D == -np.inf] = 0  # replace infs with zeros

    # find local maxima
    local_maxima = get_2D_peaks(arr2D, amp_min=DEFAULT_AMP_MIN)

    # return hashes
    return generate_hashes(local_maxima, fan_value=DEFAULT_FAN_VALUE)


def get_2D_peaks(arr2D, amp_min=DEFAULT_AMP_MIN):
    struct = generate_binary_structure(2, 1)
    neighborhood = iterate_structure(struct, PEAK_NEIGHBORHOOD_SIZE)

    # find local maxima using our filter shape
    local_max = maximum_filter(arr2D, footprint=neighborhood) == arr2D
    background = (arr2D == 0)

    eroded_background = binary_erosion(
        background,
        structure=neighborhood,
        border_value=1
    )

    # Boolean mask of arr2D with True at peaks (Fixed deprecated boolean operator by changing '-' to '^')
    detected_peaks = local_max ^ eroded_background

    # extract peaks
    amps = arr2D[detected_peaks]
    j, i = np.where(detected_peaks)

    # filter peaks
    amps = amps.flatten()
    peaks = zip(i, j, amps)
    peaks_filtered = filter(lambda x: x[2] > amp_min, peaks)  # freq, time, amp
    # get indices for frequency and time
    frequency_idx = []
    time_idx = []
    for x in peaks_filtered:
        frequency_idx.append(x[1])
        time_idx.append(x[0])

    return zip(frequency_idx, time_idx)


def generate_hashes(peaks, fan_value=DEFAULT_FAN_VALUE):
    peaks = list(peaks)
    if PEAK_SORT:
        peaks.sort(key=itemgetter(1))

    for i in range(len(peaks)):
        for j in range(1, fan_value):
            if (i + j) < len(peaks):

                freq1 = peaks[i][IDX_FREQ_I]
                freq2 = peaks[i + j][IDX_FREQ_I]
                t1 = peaks[i][IDX_TIME_J]
                t2 = peaks[i + j][IDX_TIME_J]
                t_delta = t2 - t1

                if t_delta >= MIN_HASH_TIME_DELTA and t_delta <= MAX_HASH_TIME_DELTA:
                    freq1 = bytes("{0}".format(freq1), encoding="ascii")
                    freq2 = bytes("{0}".format(freq2), encoding="ascii")
                    t_delta = bytes("{0}".format(t_delta), encoding="ascii")
                    h = sha1(
                        b", ".join([freq1, freq2, t_delta])
                    ).hexdigest()[
                        0:FINGERPRINT_REDUCTION
                        ]
                    yield (bytes("{0}".format(h), encoding="ascii"), t1)