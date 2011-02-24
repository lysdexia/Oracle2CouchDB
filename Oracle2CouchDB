#!/usr/bin/env python
# -*- coding: utf-8 -*-
#################################################################
# AUTHOR    : Doug Shawhan
# COPYRIGHT : 2011
# FILENAME  : Oracle2CouchDB 
# LICENSE   : GPL2
# http://www.gnu.org/licenses/gpl-2.0.html
#################################################################
# DESCRIPTION: Copy an oracle table into a couchdb database
# REQUIRES: python > 2.4 <3.0, cx_Oracle, couchdb
#################################################################
# USAGE: Oracle2CouchDB
# Edit the o2c.ini file, in the [oracle] section, supply your oracle
# authentication values, the oracle table to be cloned and an optional
# larger cursor_arraysize value. (A larger arraysize will often be 
# advantageous when using fetchmany())
# supply corresponding data for couchdb in the [couch] section
# 
# Future versions may allow user to enter all this on the command line.
# For right now I suggest importing OraSuck and CouchBlow into a seperate
# script. :-)
#################################################################


import cx_Oracle, datetime, sys, types, urllib2, ConfigParser, types
from subprocess import Popen, PIPE
import couchdb, couchdb.design
couchdb.json.use('cjson')

class OraSuck(object):
    def get_connection(self, db):
        self.connection = cx_Oracle.Connection(tns)
        self.cursor = cx_Oracle.Cursor(self.connection)

    def header(self, table_name):
        self.cursor.execute("select * from %s where 1=0"%table_name)
        return [i[0].lower() for i in self.cursor.description]

    def sample(self):
        rows = []
        hdr = self.header(oratable)
        cmd = """select %s from %s"""%(", ".join(hdr), oratable)

        # if we have no specified arraysize, we'll use the fetchmany() default
        # of fifty records.
        if cursor_arraysize:
            self.cursor.arraysize = int(cursor_arraysize)
        self.cursor.prepare(cmd)
        self.cursor.execute(cmd)
        while True:
            rows = [dict([(k, v) for k, v in zip(hdr, r)])
                    for r in self.cursor.fetchmany()]
            for row in rows:
                for key in row:
                    # try to convert DATE to standard ISO format
                    if hasattr(row[key], "isoformat"):
                        row[key] = datetime.datetime.isoformat(row[key])
            yield rows

class CouchBlow(object):

    def connect(self):
        couch = couchdb.Server(couch_server)

        if not all([username, password]):
            sys.exit("You'll need a username and password")

        couch.resource.credentials = (username, password)
        try:
            db = couch[oratable]
        except:
            db = couch.create(oratable)
        return db

    # use ora.sample() generator to load db
    def load(self):
        get_rows = ora.sample()
        while get_rows:
            rows = get_rows.next()
            for row in rows:
                try:
                    self.db.save(row)
                    print (row["searchtime"])
                except couchdb.http.ResourceConflict, error:
                    err = "%s: %s"%(error.message)
                    print ("%s: %s"%(row["searchtime"], err))

if __name__ == "__main__":
    config = ConfigParser.ConfigParser()
    config.optionxform = str
    config.read("o2c.ini")

    couch_server = config.get("couch", "couch_server")
    username = config.get("couch", "username")
    password = config.get("couch", "password")

    oratable = config.get("oracle", "oratable")
    oradb = config.get("oracle", "oradb")
    tns = "@".join([config.get("oracle", "tns"), oradb])
    cursor_arraysize = config.get("oracle", "cursor_arraysize")

    cb = CouchBlow()
    cb.db = cb.connect()

    ora = OraSuck()
    ora.get_connection(oradb)

    cb.load()
