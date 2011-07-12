
import os
import re
import collections
import time
import datetime

import pickle
import atexit
import nntplib
import subprocess
import shelve
import xml.sax.saxutils


class ObjectInterpolator(object):

    _MODIFIERS = {
        "quote": xml.sax.saxutils.quoteattr,
        "escape": xml.sax.saxutils.escape,
        None: lambda x: x,
    }

    def __init__(self, obj, modifiers = None):
        self._obj = obj
        self.modifiers = modifiers or self._MODIFIERS

    def __getitem__(self, key):
        modifier = None
        try:
            (key, modifier) = key.split(":", 2)
        except ValueError:
            pass

        return self._MODIFIERS[modifier](str(getattr(self._obj, key)))


class NZBVerificationException(Exception):
    pass


class NZBFileMissing(NZBVerificationException):
    pass


class NZBSegmentMissing(NZBVerificationException):
    pass


class NZBGenericCollection(collections.MutableSequence):

    _LEN_INDICATOR = None
    _LEN_EXCEPTION_STRING = "Item missing in '%(subject)s', is: %(length)s"
    _LEN_EXCEPTION = NZBVerificationException

    def __init__(self, items = None, **kwargs):
        if items is None:
            self._items = list()
        else:
            self._items = items

        for key, value in kwargs.iteritems():
            try:
                setattr(self, key, value)
            except AttributeError, e:
                raise AttributeError(
                    "%s: key: %s value: %s" % (
                        str(e),
                        key,
                        value
                    )
                )

    def __getitem__(self, item):
        return self._items[item]

    def __setitem__(self, item, value):
        self._items[item] = value

    def __len__(self):
        return len(self._items)

    def __delitem__(self, item):
        del self._items[item]

    def insert(self, index, obj):
        return self._items.insert(index, obj)

    def find(self, key, value):
        for item in self:
            if self.check(item, key, value):
                yield item

    def check(self, item, key, value):
        return getattr(item, key) == value

    def findone(self, *args, **kwargs):
        for item in self.find(*args, **kwargs):
            return item

        return None

    def split(self, key, value, good = None, bad = None):
        if good is None:
            good = NZBIndex()

        if bad is None:
            bad = NZBIndex()

        for item in self:
            if self.check(item, key, value):
                good.append(item)
            else:
                bad.append(item)

        return (good, bad)

    @property
    def complete(self):
        try:
            self.verify()
            return True
        except NZBVerificationException:
            pass

        return False

    @property
    def timestamp(self):
        return int(
            time.mktime(time.strptime(self.date, "%d %b %Y %H:%M:%S %Z"))
        )

    @property
    def items(self):
        return "\n".join([i.xml() for i in self])

    @property
    def escaped_subject(self):
        return self.subject

    @property
    def filename(self):
        return ".".join(filter(None, [self.name, self.opt, self.type]))

    @property
    def length(self):
        return len(self._items)

    @property
    def messageid(self):
        return self.mid.strip("<>")

    @property
    def groups(self):
        try:
            return "<group>%s</group>" % self.group
        except (AttributeError), e:
            return "<groups>my.default.group</group>"

    def xml(self):
        try:
            return self._XML_TEMPLATE % ObjectInterpolator(self)
        except (KeyError), e:
            raise KeyError("%s vars: %s" % (e, vars(self)))

    def verify(self):
        if self._LEN_INDICATOR:
            if int(len(self)) != int(getattr(self, self._LEN_INDICATOR)):
                raise self._LEN_EXCEPTION(
                    self._LEN_EXCEPTION_STRING % ObjectInterpolator(self),
                )

        for item in self:
            item.verify()

    @property
    def nzb_filename(self):
        return "%s.nzb" % self.name


class NZBIndex(NZBGenericCollection):

    def xml(self):
        for nzb in self:
            yield (
                nzb.nzb_filename,
                nzb.xml(),
            )


class NZB(NZBGenericCollection):
    _XML_TEMPLATE = """<?xml version="1.0" encoding="iso-8859-1" ?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.0//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.0.dtd">
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
%(items)s
</nzb>
"""

    _LEN_INDICATOR = "part_count"
    _LEN_EXCEPTION_STRING = "Files missing in '%(subject)s', is: %(length)s should be: %(part_count)s"
    _LEN_EXCEPTION = NZBFileMissing

class NZBFile(NZBGenericCollection):

    _XML_TEMPLATE = """  <file poster=%(poster:quote)s date=%(timestamp:quote)s subject=%(subject:quote)s>
    <groups>
      %(groups)s
    </groups>
    <segments>
%(items)s
    </segments>
  </file>
"""

    _LEN_INDICATOR = "segment_count"
    _LEN_EXCEPTION_STRING = "Segment missing in '%(subject)s', is: %(length)s should be: %(segment_count)s"
    _LEN_EXCEPTION = NZBSegmentMissing


class NZBSegment(NZBGenericCollection):

    _XML_TEMPLATE = '      <segment bytes=%(size:quote)s number=%(segment_number:quote)s>%(messageid:escape)s</segment>'


class Loader(object):

    def __init__(
        self,
        host,
        port,
        user,
        password,
        state = "state.pickle",
        groups = None,
    ):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._state_file = state

        self._tmpfile = "%s.yenc"
        self._decoder = "yydecode"


        # all code should be able to work with an empty state dict
        self._state = dict()

        self._groups = groups or []

        atexit.register(self.store_pickled)

        if os.path.exists(self._state_file):
            self._state = pickle.load(file(self._state_file, "rb"))

        self._server = nntplib.NNTP(
            self._host,
            self._port,
            self._user,
            self._password,
        )

    @property
    def groups(self):
        return self._groups

    @property
    def server(self):
        return self._server

    def set_state(self, group, key, value):
        #760418257 
        value = unicode(value)

        if value.isnumeric():
            value = int(value)
        elif value.isdecimal():
            value = float(value)

        self._state.setdefault(
            "groups",
            dict()
        ).setdefault(
            group,
            dict()
        )[key] = value

    def __del__(self):
        self._server.quit()

    def store_pickled(self):
        pickle.dump(self._state, file(self._state_file, "wb"))

    def fetch_body(self, aid):
        try:
            print("fetching body for aid %s" %(aid))
            tmpfile = self._tmpfile % aid
            (resp, number, daid, body) = self._server.body(aid, tmpfile)
            output = subprocess.Popen(
                [self._decoder, "--verbose", tmpfile],
                stdout = subprocess.PIPE
            ).communicate()[0]
            os.unlink(tmpfile)
            print output,
        except nntplib.NNTPTemporaryError, e:
            # ignore missing article
            pass

    def get_group_state(group, field, default):
        return self._state.get(
            "groups",
            dict(),
        ).get(
            group,
            dict(),
        ).get(
            field,
            str(default),
        )

    def set_group_state(group, field, value):
        # store last seen aid
        self._state.setdefault(
            "groups",
            dict()
        ).setdefault(
            group,
            dict()
        )[field] = value

    def catchup(self, group, aid_delta = 10000):

        # select group
        resp, count, first, last, name = self._server.group(group)

        # get last fetched aid from pickle or use a default
        start_aid = self._state.get(
            "groups",
            dict(),
        ).get(
            group,
            dict(),
        ).get(
            "last_aid",
            str(int(last) - aid_delta),
        )

        print "xover group: %s start: %s end: %s delta: %s" % (
            group,
            start_aid,
            last,
            int(last) - int(start_aid),
        )
        # get article range
        (resp, msgs) = self._server.xover(str(start_aid), last)

        # only use nzb posts via naive pattern match
        for (aid, subject, poster, date, mid, references, size, lines) in msgs:
            if ".nzb" in subject:
                self.fetch_body(aid)

        # store last seen aid
        self._state.setdefault(
            "groups",
            dict()
        ).setdefault(
            group,
            dict()
        )["last_aid"] = int(last)


_SUBJECT_RE = re.compile(
    r"""
        (?P<title>.*?)
        [[(](?P<part_number>\d+)/(?P<part_count>\d+)[])]
        \s+\-\s+(yEnc\s+)?
        "
            (?P<name>[^"]+?)
            \.?(?P<opt>sample|part\d+|vol\d+|vol\d+\+\d+)?
            \.(?P<type>nfo|avi|rar|nzb|par2|r\d+)
        "
        \s+(yEnc\s+)?
        \((?P<segment_number>\d+)/(?P<segment_count>\d+)\)
    """,
    re.I|re.X,
)


def process(article_provider, index = None, regex = _SUBJECT_RE):
    if index is None:
        index = NZBIndex()

    for data in article_provider:
        nzb = None
        nzbfile = None
        m = regex.search(data["subject"])

        if not m:
            continue

        data.update(m.groupdict())
        segment = NZBSegment(**data)

        nzb = index.findone("name", segment.name)

        if not nzb:
            nzb = NZB(**data)
            index.append(nzb)

        nzbfile = nzb.findone("filename", segment.filename)

        if not nzbfile:
            nzbfile = NZBFile(**data)
            nzb.append(nzbfile)

        nzbfile.append(segment)

    return index


