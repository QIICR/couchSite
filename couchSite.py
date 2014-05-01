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

  def __init__(self, couchDB_URL, databaseName):
    self.couchDB_URL=couchDB_URL
    self.databaseName=databaseName

    self.couch = couchdb.Server(self.couchDB_URL)

    try:
        self.db = self.couch[self.databaseName]
    except couchdb.ResourceNotFound:
        self.db = self.couch.create(self.databaseName)

  def uploadDirectoryToDocument(self,directory,documentID):
    """Walk through directory for files and copy
    them into the database as attachments to the given document
    Deleting what was there before.
    Directories paths become attachment 'filenames' so there is
    a flat list like the output of the 'find' command.
    """

    print ("uploading ", directory, " to ", documentID, " of ", self.databaseName)

    # find the database and delete the .site related
    # documents if they already exist

    # create the document
    documentJSON = self.db.get(documentID)
    #if document:
    #  self.db.delete(document)
    if not documentJSON:
      documentJSON = {
        '_id' : documentID,
        'fromDirectory' : directory,
        'attachments_md5s': {},
        '_attachments': {}
        }
    #self.db.save(documentJSON)

    '''
    for each document in the directory
      if no md5 for this document:
        put as new attachment
        include md5
      if md5 exists and is different:
        put as attachment
        update md5
        set stubs for all existing documents
      else md5 exists and is the same
        do nothing

    Note that attachments in couchdb already have md5 hashes associated, but
     I was not able to replicate these hashes using python md5 functionality.

    '''

    documentsAdded = []
    for root, dirs, files in os.walk(directory):
        for fileName in files:
            if fileName.startswith('.'):
                continue
            fileNamePath = os.path.join(root,fileName)
            print 'Processsing ',fileNamePath
            try:
                relPath = os.path.relpath(fileNamePath, directory)
                documentsAdded.append(relPath)
                from hashlib import md5
                fTmp = open(fileNamePath,'rb')
                currentmd5 = md5(fTmp.read()).digest().encode('hex')[:-1]
                if (not relPath in documentJSON['attachments_md5s'].keys()):
                  print('Uploading %s with md5 %s' % (relPath,currentmd5))
                  documentJSON['attachments_md5s'][relPath] = currentmd5
                  self.db.save(documentJSON)
                  documentJSON = self.db.get(documentID)
                  fp = open(fileNamePath,'rb')
                  self.db.put_attachment(documentJSON, fp, relPath)
                  documentJSON = self.db.get(documentID)
                elif documentJSON['attachments_md5s'][relPath] != currentmd5:
                  print('Updating %s with md5 %s' % (relPath,currentmd5))
                  documentJSON['attachments_md5s'][relPath] = currentmd5
                  self.db.save(documentJSON)
                  documentJSON = self.db.get(documentID)
                  fp = open(fileNamePath,'rb')
                  self.db.put_attachment(documentJSON, fp, relPath)
                else:
                  if documentJSON['attachments_md5s'][relPath] != currentmd5:
                    print 'Something is wrong!'

            except Exception, e:
                print ("Couldn't attach file %s" % fileNamePath)
                print str(e)
                traceback.print_exc()
                continue
              
    # go over all documents and remove those not in the current tree
    documentJSON = self.db.get(documentID)
    attachmentsList = documentJSON['attachments_md5s'].keys()
    for doc in documentsAdded:
      attachmentsList.remove(doc)
    for docToRemove in attachmentsList:
      documentJSON['attachments_md5s'].pop(docToRemove, None)
    self.db.save(documentJSON)
    documentJSON = self.db.get(documentID)
    for docToRemove in attachmentsList:
      self.db.delete_attachment(documentJSON, docToRemove)
      documentJSON = self.db.get(documentID)
      print 'Removed attachment for ', docToRemove
        
  def uploadDesignDocuments(self,directory):
    """
    For each python file in the directory create a design document based
    on the filename containing the json formatted views (map reduce
    javascript functions).
    """

    import glob
    pattern = os.path.join(directory,'*.py')
    viewFiles = glob.glob(pattern)
    for viewFile in viewFiles:
        execfile(viewFile)
        for view in views:
            viewID = os.path.join('_design',view)
            print ("uploading ", view, " of ", viewFile, " to ", self.databaseName)
            document = self.db.get(viewID)
            if document:
                self.db.delete(document)
            self.db[viewID] = views[view]


# }}}

# {{{ main, test, and arg parse

def usage():
    print ("couchSite [siteDirectory] <DatabaseName>")
    print ("couchSite [siteDirectory] <CouchDB_URL> <DatabaseName>")
    print (" CouchDB_URL default http:localhost:5984")
    print (" DatabaseName default dicom_search")

def main ():

    couchDB_URL='http://localhost:5984'
    databaseName='test'
    sitePath = sys.argv[1]

    if len(sys.argv) == 3:
        databaseName = sys.argv[2]
    if len(sys.argv) > 3:
        couchDB_URL = sys.argv[2]
        databaseName = sys.argv[3]

    uploader = CouchUploader(couchDB_URL, databaseName)
    uploader.uploadDesignDocuments(os.path.join(sitePath,"design"))
    uploader.uploadDirectoryToDocument(os.path.join(sitePath,"site"), ".site")

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
