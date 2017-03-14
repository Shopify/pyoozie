# Copyright (c) 2017 "Shopify inc." All rights reserved.
# Use of this source code is governed by a MIT-style license that can be found in the LICENSE file.
from __future__ import unicode_literals

import xmltodict


def xml_to_dict_unordered(xml):
    def unorder(value):
        if hasattr(value, 'items'):
            return {k: unorder(v) for k, v in value.items()}
        elif isinstance(value, list):
            return sorted([unorder(v) for v in value], key=str)
        else:
            return value
    return unorder(xmltodict.parse(xml))
