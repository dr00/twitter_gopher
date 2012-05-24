import os
DEFAULT_DB_FILENAME = 'CSS490_TwitterBot.sqlite'
DEFAULT_OAUTHFILE = '{}{}.twitter_oauth'.format(os.environ['HOME'], os.sep)
DEFAULT_TARGET = 'missseattleite'
DEFAULT_GOPHER = 'AnOrangeEater'
MAX_TRIES = 10
ECHO_ON = True
MAX_TIMELINE = 200

def to_datetime(datestring):
    """
    Converts the datetime string to a datetime python object.
    """
    time_tuple = parsedate_tz(datestring.strip())
    dt = datetime(*time_tuple[:6])
    return dt - timedelta(seconds=time_tuple[-1])