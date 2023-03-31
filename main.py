import logging
import warnings

import config
from scraper import *
from pathlib import *
#from config import

warnings.filterwarnings("ignore")

log = logging.getLogger("parser")
log_level = logging.INFO


class MsgCounterHandler(logging.Handler):
    level2count = None

    def __init__(self, *args, **kwargs):
        super(MsgCounterHandler, self).__init__(*args, **kwargs)
        self.level2count = {}

    def emit(self, record):
        levelname = record.levelname
        if levelname not in self.level2count:
            self.level2count[levelname] = 0
        self.level2count[levelname] += 1


def init_logs(logname="parser"):
    """ Init logging to file and stdout
    """
    dt_fmt = "%Y%m%d %H%M%S"
    # out_fmt = "{asctime}|{levelname:<1}|{name}:{message}"
    out_fmt = "%(asctime)s|%(levelname).1s|%(name)s: %(message)s"
    formatter = logging.Formatter(out_fmt, dt_fmt)
    log.setLevel(log_level)
    dt_now = datetime.datetime.now().strftime("%Y-%m-%d")
    path = Path.cwd() / 'logs'
    path.mkdir(exist_ok=True)
    fname = f"{logname}_{dt_now}.log"
    file = path / fname
    fh = logging.FileHandler(file)
    fh.setFormatter(formatter)
    log.addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    log.addHandler(ch)
    mh = MsgCounterHandler()
    log.addHandler(mh)
    return


if __name__ == '__main__':
    init_logs()
    init_db(SSH_TUNNELED)
    sql_version()
    sql_dups_delete()
    sql_dups_delete('links','link')
    sql_check_links_integrity()
    if not DEV:
        config.last_date = sql_get_last_link_date()
        for dep in config.deps:
            log.info(f'Go to {dep}')
            get_articles_links(dep)
    sql_dups_delete('links', 'link')
    links = sql_get_links()
    config.CURRENT_LINK = 0
    if links:
        if MULTITHREADED:
            multithreaded_parse_articles(links)
        else:
            parse_articles(links)
    else:
        log.info('No articles to parse')
    # Here will be uploader
    sql_dups_delete()
    sql_dups_delete('links', 'link')
    sql_check_links_integrity()
    close_db(SSH_TUNNELED)