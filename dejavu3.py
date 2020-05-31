# -*- coding: utf-8 -*-

import sys
import traceback
import multiprocessing

from helpers import find_files, read, unique_hash, \
    fingerprint_worker, fingerprint, path_to_record, \
    DEFAULT_FS, DEFAULT_WINDOW_SIZE, DEFAULT_OVERLAP_RATIO


class Dejavu:

    def __init__(self, db, limit=None):
        self.db = db
        self.limit = limit
        self.get_fingerprinted_records()

    def get_fingerprinted_records(self):
        self.recordhashes_set = set()
        for record in self.db.get_records():
            self.recordhashes_set.add(record.file_sha1)

    def fingerprint_directory(self, path, extensions=['wav'], nprocesses=None):
        try:
            nprocesses = nprocesses or multiprocessing.cpu_count()
        except NotImplementedError:
            nprocesses = 1
        else:
            nprocesses = 1 if nprocesses <= 0 else nprocesses

        pool = multiprocessing.Pool(nprocesses)

        filenames_to_fingerprint = []
        for filename, _ in find_files(path, extensions):
            if unique_hash(filename) in self.recordhashes_set:
                continue

            filenames_to_fingerprint.append(filename)

        worker_input = zip(
            filenames_to_fingerprint,
            [self.limit] * len(filenames_to_fingerprint)
        )

        iterator = pool.imap_unordered(
            fingerprint_worker,
            worker_input
        )

        while True:
            try:
                recodr_name, hashes, file_hash = iterator.next()
            except multiprocessing.TimeoutError:
                continue
            except StopIteration:
                break
            except:
                print("Failed fingerprinting")
                traceback.print_exc(file=sys.stdout)
            else:
                record_id = self.db.insert_record(recodr_name, file_hash)
                self.db.insert_hashes(record_id, hashes)
                self.db.set_record_fingerprinted(record_id)
                self.get_fingerprinted_records()

        pool.close()
        pool.join()

    def fingerprint_file(self, filepath, recodr_name=None):
        recordname = path_to_record(filepath)
        record_hash = unique_hash(filepath)
        recodr_name = recodr_name or recordname

        if record_hash in self.recordhashes_set:
            print(
                '{} already fingerprinted, continuing...'.format(recodr_name)
            )
        else:
            recodr_name, hashes, file_hash = fingerprint_worker(
                filepath,
                self.limit,
                recodr_name=recodr_name
            )
            record_id = self.db.insert_record(recodr_name, file_hash)
            self.db.insert_hashes(record_id, hashes)
            self.db.set_record_fingerprinted(record_id)
            self.get_fingerprinted_records()

    def find_matches(self, samples, fs=DEFAULT_FS):
        hashes = fingerprint(samples, fs=fs)
        return self.db.return_matches(hashes)

    def align_matches(self, matches):
        diff_counter = {}
        largest = 0
        largest_count = 0
        record_id = -1

        for tup in matches:
            rid, diff = tup
            if diff not in diff_counter:
                diff_counter[diff] = {}
            if rid not in diff_counter[diff]:
                diff_counter[diff][rid] = 0
            diff_counter[diff][rid] += 1

            if diff_counter[diff][rid] > largest_count:
                largest = diff
                largest_count = diff_counter[diff][rid]
                record_id = rid

        record = self.db.get_record_by_id(record_id)
        if record:
            record_name = record[0]
        else:
            return None

        nseconds = round(
            float(largest) / DEFAULT_FS *
            DEFAULT_WINDOW_SIZE *
            DEFAULT_OVERLAP_RATIO, 5
        )

        track = {
            'RECORD_ID': record_id,
            'RECORD_NAME': record_name,
            'CONFIDENCE': largest_count,
            'OFFSET': int(largest),
            'OFFSET_SECS': nseconds,
            'FIELD_FILE_SHA1': record[1].encode("utf8")
        }
        return track

    def search_record_by_file(self, filename):
        frames, fs, file_hash = read(filename, self.limit)

        matches = []
        for d in frames:
            matches.extend(self.find_matches(d, fs=fs))

        match = self.align_matches(matches)

        return match
