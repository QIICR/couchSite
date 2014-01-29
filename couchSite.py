#!/usr/bin/env python
"""
Upload a site directory structure to a
couch database.

Use --help to see options.
"""

import sys, os, traceback
import json, couchdb

# {{{ CouchUploader
class CouchUploader():
  """Performs the upload
  """

  def __init__(self,sitePath, couchDB_URL='http://localhost:5984', databaseName='test'):
    self.sitePath=sitePath
    self.couchDB_URL=couchDB_URL
    self.databaseName=databaseName


  def uploadDirectoryToDocument(self,directory,documentID):
    """Walk through directory for files and copy
    them into the database as attachments to the given document
    Deleting what was there before.
    Directories paths become attachment 'filenames' so there is
    a flat list like the output of the 'find' command.
    """

    # find the database and delete the .site related
    # documents if they already exist
    self.couch = couchdb.Server(self.couchDB_URL)
    self.db = self.couch[self.databaseName]

    # create the document
    document = self.db.get(documentID)
    if document:
      self.db.delete(document)
    documentJSON = {
        '_id' : documentID,
        'fromDirectory' : directory
      }
    self.db.save(documentJSON)

    # put the attachments onto the document
    for root, dirs, files in os.walk(self.sitePath):
        for fileName in files:
            if fileName.startswith('.'):
                continue
            fileNamePath = os.path.join(root,fileName)
            try:
                relPath = os.path.relpath(fileNamePath, self.sitePath)
                fp = open(fileNamePath, "rb")
                self.db.put_attachment(documentJSON, fp, relPath)
                fp.close()

            except Exception, e:
                print ("Couldn't attach file %s" % fileNamePath)
                print str(e)
                traceback.print_exc()
                continue

  def uploadDesignDocuments(self,directory):
    """
    For each json file in the directory create a design document based
    on the filename containing the json formatted views (map reduce
    javascript functions).
    """

    # find the database and delete the .site related
    # documents if they already exist
    self.couch = couchdb.Server(self.couchDB_URL)
    self.db = self.couch[self.databaseName]

    import glob
    pattern = os.path.join(directory,'*.py')
    viewFiles = glob.glob(pattern)
    for viewFile in viewFiles:
        execfile(viewFile)
        for view in views:
            # make a json compatible view with only double quotes
            # viewString = str(views[view]).replace("'",'"')
            viewString = str(views[view]).replace("'",'"')
            viewID = os.path.join('_design',view)
            document = self.db.get(viewID)
            if document:
                self.db.delete(document)
            self.db[viewID] = views[view]


# }}}

# {{{ main, test, and arg parse

def usage():
    print ("couchSite [siteDirectory] <CouchDB_URL> <DatabaseName>")
    print (" CouchDB_URL default http:localhost:5984")
    print (" DatabaseName default dicom_search")

def main ():
    global uploader
    sitePath = sys.argv[1]
    uploader = CouchUploader(sitePath)
    if len(sys.argv) > 2:
        uploader.couchDB_URL = sys.argv[2]
    if len(sys.argv) > 3:
        uploader.databaseName = sys.argv[3]

    uploader.uploadDesignDocuments(os.path.join(sitePath+"design"))
    uploader.uploadDirectoryToDocument(os.path.join(sitePath+"site"), ".site")

forIPython = """
import sys
sys.argv = ('test', '/Users/pieper/Downloads/dicom/DICOMSearch')
"""

if __name__ == '__main__':
    try:
        if len(sys.argv) < 2:
            raise BaseException('missing arguments')
        main()
    except Exception, e:
        print ('ERROR, UNEXPECTED EXCEPTION')
        print str(e)
        traceback.print_exc()

# }}}

# vim:set sr et ts=4 sw=4 ft=python fenc=utf-8: // See Vim, :help 'modeline
# vim: foldmethod=marker
