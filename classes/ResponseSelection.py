
class Response(object):
    '''
    classdocs
    '''
    name = ""
    source = None
    dest = None
    metrics = None
    conflicting_responses = None

    def __init__(self, name="undefined", src=None, dest=None, metrics=None, conflicts=None):
        '''
        Constructor
        '''
        self.name = name
        self.source = src
        self.dest = dest
        self.metrics = metrics
        self.conflicting_responses = conflicts
    def get_cost (self, metric):
        for m in self.metrics:
            if (m.name == metric):
                return m.value
        return None

class Metric(object):
    '''
    classdocs
    '''
    name = ""

    def __init__(self, name="undefined", value=-1):
        '''
        Constructor
        '''
        self.name = name
        self.value = value

class Host(object):
    '''
    classdocs
    '''
    name = ""
    htype = ""

    def __init__(self, name="undefined", htype="undefined"):
        '''
        Constructor
        '''
        self.name = name
        self.htype = htype
        
