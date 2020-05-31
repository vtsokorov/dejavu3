# -*- coding: utf-8 -*-

import os
from dejavu3 import Dejavu
from models import Database


BASEDIR = os.path.abspath(
    os.path.dirname(__file__)
)


if __name__ == "__main__":

    djv = Dejavu(
        Database(
            user='root',
            password='root',
            recreate_db=False
        )
    )

    djv.fingerprint_directory(
        os.path.join(BASEDIR, 'dataset')
    )

    record = djv.search_record_by_file(
        os.path.join(BASEDIR, 'test/short_dacha_ads.wav')
    )

    print('From file recognized: {}'.format(record))