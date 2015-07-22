#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Simple command-line sample for the Google+ API.

Command-line application that retrieves the list of the user's posts."""

from apiclient.errors import HttpError
import oauth2client
from oauth2client import client
from oauth2client.client import AccessTokenRefreshError
import time
#from oauth2client.anyjson import simplejson
from oauth2client.client import OAuth2WebServerFlow
import httplib2
from apiclient.discovery import build
from oauth2client.tools import argparser, run_flow
from oauth2client.file import Storage

import urllib2
import json

__author__ = 'antoniofsanjuan@gmail.com'
__file__ = 'client_secrets.json'

from apiclient import sample_tools


class GooglePlusService(object):

    _FS = ';' # Field Separator

    def getGooglePlusActitivyInfo(self, activity_id):

        num_retries = 3
        b_error = True

        while num_retries > 0 and b_error:

            if 0 < num_retries < 3:
                print "Retrying connection..."

            try:

                if self._gp_service is None:
                    self._gp_service, flags = sample_tools.init(
                        self._argv, 'plus', 'v1', __doc__, __file__,
                        scope='https://www.googleapis.com/auth/plus.me')

                b_error = False

            except AccessTokenRefreshError:
                print "Warming: Credential is expired. Retrying connection..."
                b_error = True
                num_retries -= 1
                self._gp_service = None
                time.sleep(5)
                continue

        comments_resource = self._gp_service.comments()
        comments_document = comments_resource.list(maxResults=500, activityId=activity_id).execute()

        if 'items' in comments_document:
          print '\t\tNumber of G+ comments: %d' % len( comments_document['items'])
          for comment in comments_document['items']:
              print '\t\tG+ Comment ID: %s' % comment['id']
              print '\t\t%s: %s' % (comment['actor']['displayName'],comment['object']['content'])
              print '\t\tG+ Likes: %d' % comment['plusoners']['totalItems']

    def printGooglePlusComment(self, comment):
        if comment is not None:
          print '\t\tG+ Comment ID: %s' % comment['id']
          print '\t\t%s: %s' % (comment['actor']['displayName'],comment['object']['content'])
          print '\t\tG+ Likes: %d' % comment['plusoners']['totalItems']

    def printCSVGooglePlusComment(self, comment):

        if comment is not None:

            id_comment = comment['id']
            author = comment['actor']['displayName']
            content = comment['object']['content']
            # Replace tabs with blanks couse problems with delimiters
            content = content.replace('\t', '   ')
            # Remove double quotes because couse problems with comment delimiters
            content = content.replace('"', '')
            content = '"%s"' % content

            dt_published = comment['published']
            num_replies = comment['plusoners']['totalItems']

            return "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s\n" % (id_comment, author, content, dt_published, num_replies)

    def getArrayGooglePlusCommentFields(self, comment):

        #print 'DEBUG: getArrayGooglePlusCommentFields() - INIT'

        if comment is not None:

            id_comment = comment['id']
            author = comment['actor']['displayName']
            content = comment['object']['content']
            #print 'DEBUG: getArrayGooglePlusCommentFields() - content: %s' % content
            # Replace tabs with blanks couse problems with delimiters
            content = content.replace('\t', '   ')
            # Remove double quotes because couse problems with comment delimiters
            content = content.replace('"', '')
            content = '"%s"' % content
            #print 'DEBUG: getArrayGooglePlusCommentFields() - content: %s' % content

            dt_published = comment['published']
            num_replies = comment['plusoners']['totalItems']

            return [id_comment, author, content, dt_published, num_replies]

    def googlePlusActitivyInfoGenerator(self, activity_id):

        #print 'DEBUG: googlePlusActitivyInfoGenerator() - activity_id: %s' % activity_id

        retries_counter = 3
        nextPageToken = None

        try:
            comments_resource = self._gp_service.comments()
            comments_document = comments_resource.list(maxResults=500, activityId=activity_id).execute()

            while comments_document is not None:

                if 'items' in comments_document:
                    for comment in comments_document['items']:
                        yield comment

                    if 'nextPageToken' in comments_document:
                        nextPageToken = comments_document['nextPageToken']

                    if nextPageToken is None or nextPageToken == "":
                        comments_document = None
                    else:
                        comments_resource.list(maxResults=500,
                                               activityId=activity_id,
                                               pageToken=nextPageToken
                        ).execute()

        except Exception as e:

            #print "\n****************************************************"
            #print "********** STATUS ERROR: %s **********" % request_error[0]['status']
            #print "****************************************************\n"

            if retries_counter > 0:
                retries_counter -= 1

            nextPageToken = comments_document['nextPageToken']


    def getActivityById(self, comment_id):

        num_retries = 3
        b_error = True

        while num_retries > 0 and b_error:

            try:
                if self._gp_service is None:
                    self._gp_service, flags = sample_tools.init(
                        self._argv, 'plus', 'v1', __doc__, __file__,
                        scope='https://www.googleapis.com/auth/plus.me')

                b_error = False

                activity_resource = self._gp_service.activities()
                activity = activity_resource.get(activityId=comment_id).execute()

            except HttpError, err:
                if err.resp.get('content-type', '').startswith('application/json'):
                    data = simplejson.loads(self.content)
                    reason = data['error']['message']

                    print "Warming: Error retrieving G+ comment. Reason: %s" % reason

                # If the error is a rate limit or connection error, wait and
                # try again.
                if err.resp.status in [403, 404, 500, 503]:
                    time.sleep(5)
                else:
                    raise
            except AccessTokenRefreshError:
                print "Warming: Credential is expired. Retrying connection..."
                b_error = True
                num_retries -= 1
                self._gp_service = None
                time.sleep(5)
                continue

        #print "\tLikes: %s" % activity['object']['plusoners']['totalItems']

        #print '\t%s' % comment['id'], comment['object']['content']
        #print '\tG+ Likes: %d' % comments_document['plusoners']['totalItems']

        return activity['object']['plusoners']['totalItems']

    def search(self, query):
        activities_resource = self._gp_service.activities()
        activities_document = activities_resource.search(maxResults=5,orderBy='best', query=query).execute()

        if 'items' in activities_document:
            ###print 'Number of Activities: %d' % len(activities_document['items'])
            for activity in activities_document['items']:
                #print activity['id'], activity['object']['content']
                self.getGooglePlusActitivyInfo(self._gp_service, activity['id'])


    def getOAuthService(self):

        num_retries = 3
        b_error = True

        while num_retries > 0 and b_error:

            if 0 < num_retries < 3:
                print "Retrying connection..."

            try:

                #self._gp_service, flags = sample_tools.init(
                #    argv, 'plus', 'v1', __doc__, __file__,
                #    scope='https://www.googleapis.com/auth/plus.me')

                # List the scopes your app requires:
                SCOPES = ['https://www.googleapis.com/auth/plus.me',
                          'https://www.googleapis.com/auth/plus.stream.write']

                # The following redirect URI causes Google to return a code to the user's
                # browser that they then manually provide to your app to complete the
                # OAuth flow.
                REDIRECT_URI = 'http://localhost:8080/'

                # For a breakdown of OAuth for Python, see
                # https://developers.google.com/api-client-library/python/guide/aaa_oauth
                # CLIENT_ID and CLIENT_SECRET come from your Developers Console project
                flow = OAuth2WebServerFlow(client_id='458527613508-8r8huecc0ab00i7ca7kngj445nt06df1.apps.googleusercontent.com',
                           client_secret='dJiglItLVeorg1ic8qd5Bk6y',
                           scope=SCOPES,
                           redirect_uri=REDIRECT_URI)


                #flow = client.flow_from_clientsecrets(
                #    'client_secrets.json',
                #    scope='https://www.googleapis.com/auth/drive.metadata.readonly',
                #    redirect_uri=REDIRECT_URI)


                storage = Storage('plus.dat')
                credentials = storage.get()

                if credentials is None or credentials.invalid:

                    print "Credential has expired. Trying to refresh..."

                    #*****
                    #flags = argparser.parse_args()
                    #credentials = run_flow(flow, storage, flags)
                    #*****

                    auth_uri = flow.step1_get_authorize_url()

                    print "G+: auth_uri = '%s'" % auth_uri

                    #*****
                    #handler = MyRedirectHandler()
                    #opener = urllib2.build_opener(handler)
                    #*****

                    opener = httplib2.Http();
                    opener.follow_all_redirects = True;
                    opener.follow_redirects = True;

                    (response, body) = opener.request(auth_uri)

                    print "response %s" % response

                    # Manual refresh entering the url by console
                    auth_code = raw_input('Enter authorization code (parameter of URL): ')
                    credentials = flow.step2_exchange(auth_code)


                http_auth = credentials.authorize(httplib2.Http())

                self._gp_service = build('plus', 'v1', http=http_auth)

                b_error = False

            except AccessTokenRefreshError:
                print "Warming: Credential is expired. Retrying connection..."
                b_error = True
                num_retries -= 1
                self._gp_service = None
                time.sleep(5)
                continue



    def __init__(self, argv):
        print "G+: ____INIT____"
        # Authenticate and construct service.
        self._gp_service = None
        self._flags = None
        self._argv = argv


        class MyRedirectHandler(urllib2.HTTPRedirectHandler):
            def http_error_302(self, req, fp, code, msg, hdrs):

                if fp.geturl().startswith('http://localhost:8080/?code'):
                    # This will raise an exception similar to this:
                    # urllib2.HTTPError: HTTP Error 302: FOUND
                    return None
                else:
                    # Let the default handling occur
                    return super(MyRedirectHandler, self).http_error_302(req, fp, code, msg, hdrs)



        self.getOAuthService()






    #    print ('The credentials have been revoked or expired, please re-run'
    #      'the application to re-authorize.')
    #  try:
    #     getCommentById('z125exihcvf1fzty504ci5nailr4h5g4bs00k')
         #search(_gp_service, 'samsung S3 opinion')

    #    person = _gp_service.people().get(userId='+JimGomes').execute()
    #
    #    print 'Got your ID: %s' % person['displayName']
    #    print
    #    print '%-040s -> %s' % ('[Activitity ID]', '[Content]')
    #
    #    # Don't execute the request until we reach the paging loop below.
    #    request = _gp_service.activities().list(
    #        userId=person['id'], collection='public')
    #
    #    comments_resource = _gp_service.comments()
    #
    #    # Loop over every activity and print the ID and a short snippet of content.
    #    while request is not None:
    #      activities_doc = request.execute()
    #
    #
    #      for item in activities_doc.get('items', []):
    #        print '%-040s -> %s' % (item['id'], item['object']['content'][:30])
    #        print '%-040s -> %s' % (item['id'], item['title'][:30])
    #
    #
    #      activity_id = activities_doc['items'][0]

          #sys.stdout.write('activity_id = %s\n' % activity_id['id'])
          #print '%-040s -> %s' % (activity_id['id'], activity_id['title'][:30])

          #comments_document = comments_resource.list( \
          #  maxResults=10,activityId=activity_id).execute()

          #if 'items' in comments_document:
          #  print 'got page with %d' % len( comments_document['items'] )
          #  for comment in comments_document['items']:
          #    print comment['id'], comment['object']['content']

    #    request = _gp_service.activities().list_next(request, activities_doc)

    #  except client.AccessTokenRefreshError:

    #    print ('The credentials have been revoked or expired, please re-run'
    #      'the application to re-authorize.')

