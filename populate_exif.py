#!/usr/bin/env python
# encoding: utf-8

"""
Problem Statement

Using any language and data-store of your choice, write an application that
reads a set of photos from a network store (S3), parses the EXIF data from
the photos and indexes the EXIF key/value pairs into a query-able store by
unique photo.
"""

import exifread
import time
from multiprocess import MultiProcess
from multiprocess import ResultProcessor
import requests
from tinydb import TinyDB, Query
import xml.etree.ElementTree as etree
import urllib, cStringIO


__author__ = 'mrafi@mrafi.in (Mohammad Rafi)'

DB_NAME = 'database.db'
URL = "http://s3.amazonaws.com/waldo-recruiting"
XML_NAMESPACE = {'base': 'http://s3.amazonaws.com/doc/2006-03-01/'}
MAX_RETRIES = 3


def download_xml(url, retries=0):
    retries += 1
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    elif retries > MAX_RETRIES:
        return None
    return download_xml(url, retries)


def get_image_list(url):
    """Get image list from the xml.
    :returns: list of images to be downloaded

    """
    xml = download_xml(url)
    tree = etree.fromstring(xml)
    image_nodes = [i.text for i in
            tree.findall("base:Contents/base:Key", XML_NAMESPACE)]
    return image_nodes


class ResultDbProcessor(ResultProcessor):
    """Result Processors"""

    def __init__(self, result_queue, num_consumers):
        # Initiate DB.
        super(ResultDbProcessor, self).__init__(result_queue, num_consumers)
        self.db = self._init_db()

    def _init_db(self):
        db = TinyDB(DB_NAME)
        return db

    def process_result(self, next_result):
        """Overwrite the result process callback."""
        # The result is a dict containing instances for exif values.
        # For clarity sake we are converting it to str
        clean_result = dict(
                zip(next_result.keys(), map(str, next_result.values())))
        self.db.insert(clean_result)


class ExtractExifTask(object):
    """Exif extraction task class."""

    def __init__(self, image_id):
        self.image_id = image_id

    def __call__(self):
        return self.call()  # pretend to take some time to do our work

    def __str__(self):
        return '%s * %s' % (self.image_id)

    def call(self):
        """The actual meat."""
        # TODO: Add retrials
        url = URL + '/' + self.image_id
        f = cStringIO.StringIO(urllib.urlopen(url).read())
        tags = exifread.process_file(f)
        return tags


def main():
    """Main entry point. """
    # TODO: Do logging?
    print 'Downloading the XML'
    image_list = get_image_list(URL)
    print 'Images to process: ', len(image_list)
    m = MultiProcess()
    m.prepare_consumer()
    m.prepare_result_consumer(ResultDbProcessor)
    print 'Queuing the jobs.'
    for i in image_list:
        m.enqueue_job(ExtractExifTask(i))
    m.add_end_jobs()
    # Progress.
    while True:
        if m.tasks.empty():
            break
        cnt = m.tasks.qsize()
        print 'Tasks Left: ', cnt
        time.sleep(10)



if __name__ == "__main__":
    main()
