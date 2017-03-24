# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

import uuid

import mock
import pytest

from six import moves


@pytest.fixture
def fake_uuid4(monkeypatch):
    def preset_uuid4s():
        for number in moves.range(0, 10 ** 8):
            yield uuid.UUID('{:08x}-0000-0000-0000-000000000000'.format(number))
    mock_uuid = mock.Mock()
    mock_uuid.side_effect = preset_uuid4s()
    monkeypatch.setattr('uuid.uuid4', mock_uuid)
