
class rkey(object):
    """This is a nasty little hack that i'm only allowing because i'm jetlagged!"""
    def __new__(cls, *args):
        return ':'.join(map(str, args))
    
    @classmethod
    def split(cls, key):
        return key.split(':')
        
