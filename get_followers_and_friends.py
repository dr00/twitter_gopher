#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
from gopherbot import GopherBot
from gbglobals import *
import sqlite3
import sys
import argparse
import os
from time import gmtime, strftime



def _main(argv=None):
    """
    Main method of the module when run from the command line.
    """
    # Get the command line arguments
    args = process_command_line(argv)
    gopher_screen_name = '@{0}'.format(args.gopher_screen_name)
    oauth_filename = args.oauth_filename
    db_filename = args.db_filename
    target_screen_name = args.target_screen_name

    if sys.flags.debug:
        print('gopher_screen_name: {0}'.format(gopher_screen_name))
        print('oauth_filename:     {0}'.format(oauth_filename))
        print('db_filename:        {0}'.format(db_filename))
        print('target_screen_name: {0}'.format(target_screen_name))

    dt = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    print('{0}, {1}, {2}, {3}, {4}'.format(dt, gopher_screen_name, oauth_filename, db_filename, target_screen_name))

    bot = GopherBot(gopher_screen_name, oauth_filename)
	#db_is_new = not os.path.exists(db_filename)
    db_exists = os.path.exists(DEFAULT_SCHEMAFILE)
    if not db_exists:
	    raise RuntimeError('Db does not exists')	    

    with sqlite3.connect(db_filename) as conn:
		if db_is_new:
			print('Creating schema...')
			with open(schema_filename, 'rt') as f:
				schema = f.read()
			conn.executescript(schema)

        cursor = conn.cursor()
        friends = bot.get_friends(target_screen_name)
        SQL = """
	          INSERT INTO bot_friends(user_id) VALUES (?)
	          """
        for f in friends['ids']:
            cursor.execute(SQL, (f,))

        followers = bot.get_followers(target_screen_name)
        SQL = """
	          INSERT INTO bot_followers(user_id) VALUES (?)
              """
        for f in followers['ids']:
            cursor.execute(SQL, (f,))

    return 0


def process_command_line(argv):
    """
    Returns a Namespace object with the argument names as attributes.
    `argv`` is a list of arguments, or `None` for ``sys.argv[1:]``.
    """

    if argv is None:
        argv = sys.argv[1:]

    # initialize the parse object
    parser = argparse.ArgumentParser(description='Print Twitter statuses.')

    g_help = 'The screen_name of the account to use for getting data'
    parser.add_argument('-g', action='store',
                        dest='gopher_screen_name',
                        default=DEFAULT_GOPHER,
                        help=g_help)

    oa_help = 'The filename that has the oauth_token and oauth_token_secret'
    parser.add_argument('-o', action='store',
                        dest='oauth_filename',
                        default=DEFAULT_OAUTHFILE,
                        help=oa_help)

    d_help = 'The database name to store retrieved data'
    parser.add_argument('-d', action='store',
                        dest='db_filename',
                        default=DEFAULT_DB_FILENAME,
                        help=d_help)

    t_help = 'The screen_name of the account to monitor'
    parser.add_argument('-t', action='store',
                        dest='target_screen_name',
                        default=DEFAULT_TARGET,
                        help=t_help)

    return parser.parse_args()



if __name__ == '__main__':
    status = _main()
    sys.exit(status)
