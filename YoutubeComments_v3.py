#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'antoniofsanjuan'

from mysql.connector import errorcode

import gdata.service

import mysql.connector
import time
import datetime
import HTMLParser
import MySQLdb

import chardet

class GoogleCommentsService(object):

    MAX_RESULTS = 40
    _COMMENTS_COUNT = 0
    _TEXT_CHARSET = 'utf-8'

    _FS = ';' # Field Separator

    def __init__(self, argv):
        print ""

    def force_decode(self, string):

        codecs=['utf_8', 'ascii', 'latin_1', 'iso8859_2', 'utf_16', 'cp1250', 'cp1251', 'cp1252', 'iso8859_3', 'iso8859_4', 'iso8859_5', 'big5',
                                            'cp857', 'cp860', 'cp861', 'cp862', 'cp863', 'cp864', 'cp865', 'cp866', 'cp869', 'cp874', 'cp875', 'cp932', 'cp949', 'cp950',
                                            'cp1006', 'cp1026', 'cp1140', 'cp1253', 'cp1254', 'cp1255', 'cp1256', 'cp1257', 'cp1258', 'euc_jp', 'big5hkscs', 'cp037', 'cp424', 'cp437',
                                            'euc_jis_2004', 'euc_jisx0213', 'euc_kr', 'gb2312', 'gbk', 'gb18030', 'hz', 'iso2022_jp', 'iso2022_jp_1', 'iso2022_jp_2',
                                            'iso2022_jp_2004', 'iso2022_jp_3', 'iso2022_jp_ext', 'iso2022_kr', 'cp500', 'cp737', 'cp775', 'cp850', 'cp852', 'cp855', 'cp856',
                                            'iso8859_6', 'iso8859_7', 'iso8859_8', 'iso8859_9', 'iso8859_10', 'iso8859_13', 'iso8859_14', 'iso8859_15', 'johab', 'koi8_r',
                                            'koi8_u', 'mac_cyrillic', 'mac_greek', 'mac_iceland', 'mac_latin2', 'mac_roman', 'mac_turkish', 'ptcp154', 'shift_jis',
                                            'shift_jis_2004', 'shift_jisx0213', 'utf_16_be', 'utf_16_le', 'utf_7']
        for i in codecs:
            try:
                return string.decode(i)
            except:
                pass

        return string

    def detect_encoding(self, string):

        codecs=['utf_8', 'ascii', 'latin_1', 'iso8859_2', 'utf_16', 'cp1250', 'cp1251', 'cp1252', 'iso8859_3', 'iso8859_4', 'iso8859_5', 'big5',
                                            'cp857', 'cp860', 'cp861', 'cp862', 'cp863', 'cp864', 'cp865', 'cp866', 'cp869', 'cp874', 'cp875', 'cp932', 'cp949', 'cp950',
                                            'cp1006', 'cp1026', 'cp1140', 'cp1253', 'cp1254', 'cp1255', 'cp1256', 'cp1257', 'cp1258', 'euc_jp', 'big5hkscs', 'cp037', 'cp424', 'cp437',
                                            'euc_jis_2004', 'euc_jisx0213', 'euc_kr', 'gb2312', 'gbk', 'gb18030', 'hz', 'iso2022_jp', 'iso2022_jp_1', 'iso2022_jp_2',
                                            'iso2022_jp_2004', 'iso2022_jp_3', 'iso2022_jp_ext', 'iso2022_kr', 'cp500', 'cp737', 'cp775', 'cp850', 'cp852', 'cp855', 'cp856',
                                            'iso8859_6', 'iso8859_7', 'iso8859_8', 'iso8859_9', 'iso8859_10', 'iso8859_13', 'iso8859_14', 'iso8859_15', 'johab', 'koi8_r',
                                            'koi8_u', 'mac_cyrillic', 'mac_greek', 'mac_iceland', 'mac_latin2', 'mac_roman', 'mac_turkish', 'ptcp154', 'shift_jis',
                                            'shift_jis_2004', 'shift_jisx0213', 'utf_16_be', 'utf_16_le', 'utf_7']
        for i in codecs:
            try:
                string.decode(i)
                return i
            except:
                pass

        return "unicode"

    def formatYoutubeDate(self, yt_datetime):
        _tmp = time.strptime(yt_datetime[:-5], '%Y-%m-%dT%H:%M:%S')
        formated_time = datetime.datetime(*_tmp[:6])

        return formated_time

    def printCSVYoutubeComment(self, json_comment, video_id, gp_likes_activity):

        csv_format_string = "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s" + self._FS + "%s" + self._FS + "%s" + self._FS + "%s\n"

        #print "\nprintCSVYoutubeComment() - INIT"

        yt_comment = json_comment["snippet"]["topLevelComment"]

        author = yt_comment["snippet"]["authorDisplayName"]
        #print "\tauthor: %s" % author
        text = yt_comment["snippet"]["textDisplay"]
        #print "\ttext: %s" % text
        likes = yt_comment["snippet"]["likeCount"]
        #print "\tlikes: %s" % likes
        yt_comment_id = yt_comment["id"]
        #print "\tyt_comment_id: %s" % yt_comment_id
        yt_published = yt_comment["snippet"]["publishedAt"]
        #print "\tyt_published: %s" % yt_published

        yt_reply_count = int( json_comment["snippet"]['totalReplyCount'] )
        #print "\tyt_reply_count: %s" % yt_reply_count

        yt_author_name = author
        #print "\tyt_author_name: %s" % yt_author_name

        #print "Text: %s " % text
        #print "\ttext isInstance of STR? %s; type? %s" % ( isinstance(text, str), type(text) )
        #### yt_content = text.encode('utf-8')
        yt_content = yt_comment["snippet"]["textDisplay"]
        #print "2"
        #print "\tyt_content: %s" % yt_content

        # Remove BOM character
        #yt_content = yt_content.replace('\xef\xbb\xbf', '')
        #print "3"
        #print "\tyt_content without BOM: %s" % yt_content

        # Remove double quotes because couse problems with comment delimiters
        yt_content = yt_content.replace('"', '')
        #print "4"
        yt_content = '"%s"' % yt_content

        #print "yt_content: %s" % yt_content

        ### KEY POINT: Check 1 per 1 field if string codification is Unicode or String. Convert them if necessary
        ### Python is not able to concatenate string in different codifications
        #yt_content = yt_content.decode("utf-8") if isinstance(yt_content, str) else unicode(yt_content)
        yt_comment_id = yt_comment_id.decode("utf-8") if isinstance(yt_comment_id, str) else unicode(yt_comment_id)

        #yt_author_name = yt_author_name.decode("utf-8") if isinstance(yt_author_name, str) else unicode(yt_author_name)
        try:
            #chardet_encode = chardet.detect(yt_author_name)['encoding']
            #print "\nChardet 'yt_author_name' enconding: %s" % chardet_encode

            #print "Force Dectect: %s" % self.detect_encoding(yt_author_name)
            yt_author_name_decoded = self.force_decode(yt_author_name)
            #print "yt_author_name_decoded: %s" % yt_author_name_decoded
            yt_author_name = yt_author_name_decoded
        except Exception as e:
            print "No se puede codificar en 'utf8' el nombre de autor: %s\n" % yt_author_name
            print "Excepcion:\n%s" % e


        try:
            #chardet_encode = chardet.detect(yt_content)['encoding']
            #yt_content = yt_content.decode(chardet_encode).encode('utf8')
            #print "\nChardet enconding: %s" % chardet_encode

            # DEBUG
            #print "Force Dectect: %s" % self.detect_encoding(yt_content)
            '''
            if isinstance(yt_content, str):
                print "\tisInstance of Str? %s" % isinstance(yt_content, str)
            elif isinstance(yt_content, unicode):
                print "\tisInstance of Unicode? %s" % isinstance(yt_content, unicode)
            '''

            # Detect encoding and convert to unicode or leave in unicode format
            if isinstance(yt_content, str):
                yt_content_decoded = self.force_decode(yt_content)
            else:
                #yt_content_decoded = unicode(yt_content)
                yt_content_decoded = yt_content

            #print "\tQuitando BOM..."
            #print "\tContent: $%s$" % yt_content_decoded
            #yt_content = yt_content_decoded.replace('\xef\xbb\xbf', '')
            yt_content = yt_content_decoded.encode('utf-8-sig')
            yt_content = yt_content.decode('utf-8-sig')
            #print "\tBOM eliminado."
            #print "\tContent w/o BOM: $%s$" % yt_content

        except Exception as e:
            print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
            print "No se puede codificar en 'utf8' el contenido: %s\n" % yt_content
            print "Excepcion:\n%s" % e
            print "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n"
            exit(1)

        yt_published = yt_published.decode("utf-8") if isinstance(yt_published, str) else unicode(yt_published)
        yt_reply_count = yt_reply_count.decode("utf-8") if isinstance(yt_reply_count, str) else unicode(yt_reply_count)
        gp_likes_activity = gp_likes_activity.decode("utf-8") if isinstance(gp_likes_activity, str) else unicode(gp_likes_activity)
        video_id = video_id.decode("utf-8") if isinstance(video_id, str) else unicode(video_id)

        ts = time.time()
        str_stored_timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

         # Uncomment to Debug
        '''
        print "yt_comment_id: %s" % yt_comment_id
        print "yt_author_name: %s" % yt_author_name
        print "yt_content: %s" % yt_content
        print "yt_published: %s" % self.formatYoutubeDate(yt_published)
        print "yt_reply_count: %s" % yt_reply_count
        print "gp_likes_activity: %s" % gp_likes_activity
        print "video_id: %s" % video_id
        '''

        return csv_format_string % (yt_comment_id, yt_author_name, yt_content,
                                    self.formatYoutubeDate(yt_published), str_stored_timestamp, yt_reply_count, gp_likes_activity, video_id)

    def printCSVGooglePlusComment(self, gp_service, gp_comment, yt_comment_id, num_replies, video_id):

        #print 'DEBUG: printCSVGooglePlusComment() - INIT'

        csv_format_string = "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s"+ self._FS + "%s" + self._FS + "%s" + self._FS + "%s" + self._FS + "%s \n"
        arr_gp_comment_fields = gp_service.getArrayGooglePlusCommentFields(gp_comment)

        #htmlParser = HTMLParser.HTMLParser()

        formatted_published_time = self.formatYoutubeDate(arr_gp_comment_fields[3])
        #parsed_comment_body = htmlParser.unescape(arr_gp_comment_fields[2])
        parsed_comment_body = self.force_decode(arr_gp_comment_fields[2])

        ###gp_author = arr_gp_comment_fields[1].decode("utf-8") if isinstance(arr_gp_comment_fields[1], str) else unicode(arr_gp_comment_fields[1])
        gp_author = self.force_decode(arr_gp_comment_fields[1])

        ts = time.time()
        str_stored_timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        return csv_format_string % (arr_gp_comment_fields[0], gp_author, parsed_comment_body,
                                    formatted_published_time, str_stored_timestamp, arr_gp_comment_fields[4],
                                    yt_comment_id, num_replies, video_id)

    def executeLoadInBD(self, query):
        try:

            yt_conn = MySQLdb.connect('127.0.0.1', 'root', 'cc1251', 'youtube')
            #print "\nConexion con mysql establecida"
            yt_cursor = yt_conn.cursor()

            yt_cursor.execute(query)

            yt_conn.commit()

            # Uncomment to Debug
            total = long(str(vars(yt_cursor)['_info']).split(':')[1].strip(' ').split(' ')[0])
            skipped = long(str(vars(yt_cursor)['_info']).split(':')[3].strip(' ').split(' ')[0])

            print "********************************************************"
            # print vars(yt_cursor)
            print "Query: %s" % vars(yt_cursor)['_executed']
            print "Result: %s" % vars(yt_cursor)['_info']
            print "Loaded: %s" % str(total - skipped)
            print "********************************************************"


            yt_cursor.close()
            yt_conn.close()

        except (KeyboardInterrupt, SystemExit):
            yt_conn.rollback()
            yt_cursor.close()
            yt_conn.close()
            print "\nRolling back database changes..."
            print "\n\nShutting down youtube-gatherer..."
            raise

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("*** ERROR ***: Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("*** ERROR ***: Database does not exists")
            elif err.errno == errorcode.CR_CONN_HOST_ERROR:
                print("*** ERROR ***: Database is not running")
            else:
                print(err)

            if yt_conn is not None:
                yt_conn.close()


    # Call the API's commentThreads.list method to list the existing comment threads.
    def get_comment_threads(self, yt_service, video_id, next_page):

        #print "get_comment_threads(): video_id = %s\n" % video_id

        if next_page is None:
            #print "get_comment_threads(): \tnext_page IS NONE"
            results = yt_service.commentThreads().list(
                part='id,snippet',
                videoId=video_id,
                maxResults=100,
                order='time',
                textFormat='plainText',
                key='usKbvXnvKS9XA377cncN0oZJ'
            ).execute()
        else:
            #print "get_comment_threads(): \tnext_page IS:%s" % next_page
            results = yt_service.commentThreads().list(
                part='id,snippet',
                videoId=video_id,
                maxResults=100,
                order='time',
                textFormat='plainText',
                pageToken=next_page,
                key='usKbvXnvKS9XA377cncN0oZJ'
            ).execute()

        ## Uncomment to DEBUG
        #print "TOTAL: %s" % len(results['items'])

        #for item in results["items"]:
        #  comment = item["snippet"]["topLevelComment"]
        #  author = comment["snippet"]["authorDisplayName"]
        #  text = comment["snippet"]["textDisplay"]
        #  print "Comment by %s: %s" % (author, text)

        return results


    # Call the API's comments.list method to list the existing comment replies.
    def get_comments(self, yt_service, video_id):

        results = youtube.comments().list(
            part="id,snippet",
            videoId=parent_id,
            textFormat="plainText",
            key='usKbvXnvKS9XA377cncN0oZJ'
        ).execute()

        #for item in results["items"]:
        #    author = item["snippet"]["authorDisplayName"]
        #    text = item["snippet"]["textDisplay"]
        #    print "Comment by %s: %s" % (author, text)

        return results


    # Retrieves comments from a specific video comment thread
    def comments_generator(self, yt_service, video_id):

        #print "comments_generator: *** INIT ***"

        retries_counter = 3

        video_comment_threads = self.get_comment_threads(yt_service, video_id, None)

        comments_count = 0
        next_page = None
        while video_comment_threads is not None and (retries_counter > 0):
            try:
                num_retries_nextPageToken = 3

                for item in video_comment_threads["items"]:

                    comments_count += 1
                    #print "\tComments #%s\tID: %s\tPublished At: %s" % (comments_count, item['id'], item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
                    yield item

                if 'nextPageToken' in video_comment_threads:
                    next_page = video_comment_threads['nextPageToken']
                    # DEBUG print "\nnext_page[%s]: %s" % (comments_count, next_page)
                else:
                    # Intentamos recuperar otras 3 veces el nextPageToken por si se hubiera recuperado mal
                    # DEBUG print "\n*****************************************************\n"
                    num_retries_nextPageToken = 3
                    video_comment_thread_tmp = ""
                    while num_retries_nextPageToken > 0 and 'nextPageToken' not in video_comment_thread_tmp:
                        # DEBUG print "\tIntentando recuperar de nuevo nextPageToken(%s): %s" % (comments_count, next_page)
                        video_comment_thread_tmp = self.get_comment_threads(yt_service, video_id, next_page)

                        if 'nextPageToken' in video_comment_thread_tmp:
                            next_page = video_comment_thread_tmp['nextPageToken']
                            video_comment_threads = video_comment_thread_tmp
                            # DEBUG print "\t\tEncontrado nuevo pageToken: %s" % next_page
                        else:
                            num_retries_nextPageToken -= 1
                            time.sleep(3)


                if next_page is None or next_page == "" or num_retries_nextPageToken == 0:
                    # DEBUG print "No hemos encontrado next_page por lo que salimos..."
                    video_comment_threads = None
                else:
                    # DEBUG print "Hemos entoncontrado next_page, continuamos..."
                    video_comment_threads = self.get_comment_threads(yt_service, video_id, next_page)

            except gdata.service.RequestError, request_error:
                    # DEBUG print "\n****************************************************"
                    # DEBUG print "********** STATUS ERROR: %s **********" % request_error[0]['status']
                    # DEBUG print "****************************************************\n"

                    if retries_counter > 0:
                        retries_counter -= 1
        #                ''' Uncomment to debug
                        #print "Retrying extraction from index of comments [%s]..." % comments_count
        #                '''
                    next_page = video_comment_threads['nextPageToken']

