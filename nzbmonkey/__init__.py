
import os
import re
import collections
import time
import datetime

import pickle
import atexit
import nntplib
import subprocess
import xml.sax.saxutils

import logging


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


class NZBChecker(object):

    _RE_TYPE = type(re.compile(""))

    def __init__(self, value):
        # since __*__ methods are look up on the class/type, we need to create 
        # a new type for every instance, and assign to the classes' __call__
        self.__class__ = type(self.__class__.__name__, (self.__class__,), {})


        if isinstance(value, self._RE_TYPE):
            self.__class__.__call__ = lambda self, x: value.search(x)
        elif callable(value):
            self.__class__.__call__ = lambda self, x: value(x)
        else:
            self.__class__.__call__ = lambda self, x: value == x


def NZBCheckValue(value):
    def check_value(x, value = value):
        return value == x
    return check_value


def NZBCheckRe(value):
    def check_re(x, value = value):
        if value.search(x):
            return True
        else:
            return False

    return check_re


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

    def __iter__(self):
        return iter(self._items)

    def find(self, key, check):
        #rounds = 0
        for item in self:
            if check(getattr(item, key)):
                yield item
            #rounds += 1

        #print "check(%d in %.3fs): %s = %s" % (
        #    rounds,
        #    time.time() - t_start,
        #    key,
        #    value,
        #)

    def findone(self, *args, **kwargs):
        for item in self.find(*args, **kwargs):
            return item

        return None

    def split(self, key, check, good = None, bad = None):
        #check = NZBChecker(value)
        if good is None:
            good = NZBIndex()

        if bad is None:
            bad = NZBIndex()

        for item in self:
            if check(getattr(item, key)):
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
        for fmt in ["%d %b %Y %H:%M:%S %Z", "%d, %b %Y %H:%M:%S %Z"]:
            try:
                return int(
                    time.mktime(time.strptime(self.date, fmt))
                )
            except ValueError, e:
                pass
        # fallback to now
        return int(time.time())

    @property
    def items(self):
        return "\n".join([i.xml() for i in self])

    @property
    def escaped_subject(self):
        return self.subject

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

    @property
    def nzb_filename(self):
        return "%s.nzb" % self.name

    def xml(self):
        try:
            return self._XML_TEMPLATE % ObjectInterpolator(self)
        except (KeyError), e:
            raise KeyError("%s vars: %s" % (e, vars(self)))

    def verify(self):
        if self._LEN_INDICATOR:
            field = getattr(self, self._LEN_INDICATOR)
            if isinstance(field, type(None)):
                return False
            if int(len(self)) < int(field):
                raise self._LEN_EXCEPTION(
                    self._LEN_EXCEPTION_STRING % ObjectInterpolator(self),
                )

        for item in self:
            item.verify()


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

    @property
    def parts(self):
        if self._LEN_INDICATOR:
            return getattr(self, self._LEN_INDICATOR)

        return None


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


class Loader(collections.MutableMapping):

    def __init__(
        self,
        host,
        port,
        user,
        password,
        state = "state.pickle",
        groups = None,
        delta = 50000,
        max_delta = 500000,
    ):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._state_file = state
        self._delta = int(delta)
        self._max_delta = int(max_delta)

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

    def __getitem__(self, key):
        return self._state[key]

    def __setitem__(self, key, value):
        self._state[key] = value

    def __len__(self):
        return len(self._state)

    def __delitem__(self, key):
        del self._state[key]

    def __contains__(self, key):
        return key in self._state

    def __iter__(self):
        return iter(self._state)

    def __del__(self):
        self._server.quit()

    def store_pickled(self):
        pickle.dump(self._state, file(self._state_file, "wb"))

    def get_group_state(self, group, field, default):
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

    def set_group_state(self, group, field, value):
        # store last seen aid
        self._state.setdefault(
            "groups",
            dict()
        ).setdefault(
            group,
            dict()
        )[field] = value

    def catchup(self, groups = None, delta = None, persist = True):

        if groups is None:
            groups = self.groups

        for group in groups:
            t_start = time.time()
            # select group
            resp, count, first, last, name = self._server.group(group)
            last = int(last)

            # get last fetched aid from pickle or use a default
            last_aid = int(
                self.get_group_state(group, "last_aid", 0)
            )

            if not delta:
                if last_aid == 0:
                    logging.info(
                        "catchup - group %s has unknown last article, using default delta: %d",
                        group,
                        self._delta,
                    )
                    last_aid = last - self._delta

                fetch_delta = min(last - last_aid, self._max_delta)
            else:
                fetch_delta = int(delta)

            start_aid = str(last - int(fetch_delta))

            logging.info(
                "catchup - group: %s last: %s start: %s end: %s fetch-delta: %s real-delta: %s",
                group,
                last_aid,
                start_aid,
                last,
                fetch_delta,
                int(last) - last_aid,
            )

            if fetch_delta == 0:
                continue
            # get article range
            (resp, msgs) = self._server.xover(str(start_aid), str(last))

            # only use nzb posts via naive pattern match
            articles = 0
            for (aid, subject, poster, date, mid, references, size, lines) in msgs:
                yield dict(
                    aid = aid,
                    subject = subject,
                    poster = poster,
                    date = date,
                    mid = mid,
                    references = references,
                    size = size,
                    lines = lines,
                    group = group,
                )
                articles += 1


            # store last seen aid
            if persist:
                self.set_group_state(group, "last_aid", int(last))

            logging.info(
                "catchup - group: %s duration: %.3fs articles: %d",
                group,
                time.time() - t_start,
                articles,
            )


_SUBJECT_RE = re.compile(
    r"""^
        (?P<title>.*?)
        (?:\s*\-\s*)?
        (:?[[(](?P<part_number>\d+)/(?P<part_count>\d+)[])])?
        (yEnc)?
        (?:\s*\-\s*)?
        "(?P<filename>
            (?P<name>[^"]+?)
            \.?(?P<opt>(?:xvid-|sample-|nfo.)sample|part\d+|vol\d+|vol\d+\+\d+)?
            \.(?P<type>[^."]+)
        )"
        \s+(yEnc\s+)?
        \((?P<segment_number>\d+)/(?P<segment_count>\d+)\)
    """,
    re.I|re.X,
)

_STATS = dict(
    total = 0,
    discarded = 0,
    considered = 0,
    nzbs = 0,
    nzbfiles = 0,
    segments = 0,
    name_misses = 0,
    title_misses = 0,
)


def preprocess(article_provider, regex = _SUBJECT_RE):

    logging.info("preprocessing articles")
    for data in article_provider:
        _STATS["total"] += 1
        m = regex.match(data["subject"])

        if not m:
            logging.debug("process - discard: %s", data["subject"])
            _STATS["discarded"] += 1
            continue

        logging.debug("process - consider: %s",  data["subject"])
        _STATS["considered"] += 1
        data.update(m.groupdict())
        yield data


def process(article_provider, index = None):
    if index is None:
        index = NZBIndex()

    logging.info("processing articles")
    try:
        for data in article_provider:
            nzb = None
            nzbfile = None

            segment = NZBSegment(**data)
            _STATS["segments"] += 1


            #logging.debug("process - new segment: %s", segment.subject)
            nzb = index.findone(
                "name",
                NZBCheckValue(segment.name),
            )

            if not nzb:
                logging.debug(
                    "process - could not find a match with name: %s, checking for title: %s",
                    segment.name,
                    segment.title,
                )
                _STATS["name_misses"] += 1
                nzb = index.findone(
                    "title",
                    NZBCheckValue(segment.title),
                )
                if nzb:
                    logging.debug("process - found nzb: %s", nzb.subject)

            fresh = False
            if not nzb:
                nzb = NZB(**data)
                logging.debug(
                    "process - new nzb: %s subject: %s",
                    nzb.name,
                    nzb.subject,
                )
                _STATS["title_misses"] += 1
                _STATS["nzbs"] += 1
                index.append(nzb)
                fresh = True

            if not fresh:
                nzbfile = nzb.findone(
                    "filename",
                    NZBCheckValue(segment.filename),
                )

            if not nzbfile:
                nzbfile = NZBFile(**data)
                logging.debug(
                    "process - new nzbfile: %s subject: %s",
                    nzbfile.filename,
                    nzbfile.subject,
                )
                _STATS["nzbfiles"] += 1
                nzb.append(nzbfile)

            nzbfile.append(segment)
    except (SystemExit, KeyboardInterrupt), e:
        logging.info(
            "process - stats %s",
            " ".join([" :".join((x,str(y))) for x,y in _STATS.iteritems()])
        )
        raise e

    logging.info(
        "process - stats %s",
        " ".join([": ".join((x,str(y))) for x,y in _STATS.iteritems()])
    )

    return index


