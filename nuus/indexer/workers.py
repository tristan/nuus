from nuus import usenet, usenet_pool, utils
import os

NEW_ARTICLES_DIR=os.path.join('cache','new')

def global_initialise():
    try:
        os.makedirs(NEW_ARTICLES_DIR)
    except OSError as e:
        if e.errno != 17:
            raise
        
def convert(s):
    """decodes latin-1 and re-encodes as utf-8"""
    return s.decode('latin-1').encode('utf-8')

def UsenetWorker(group, start, end):
    u = usenet.Usenet(connection_pool=nuus.usenet_pool)
    articles = u.get_articles(group, start, end)
    if articles:
        alines = []
        glines = []
        for number, subject, poster, sdate, id, _, size, _ in articles:
            alines.append(convert('%s\n' % '\t'.join([id[1:-1], subject, poster, str(parse_date(sdate)), size])))
            glines.append(convert('%s\t%s\n' % (group, id[1:-1])))
        with gzip.open(os.path.join(NEW_ARTICLES_DIR, '%s-articles.%s.gz' % (group, start))) as f:
            f.writelines(alines)
        with gzip.open(os.path.join(NEW_ARTICLES_DIR, '%s-group_article_map.%s.gz' % (group, start))) as f:
            f.writelines(glines)
