class Message():
    def __init__(self, mType, mID, mMessage):
        self.type = mType
        self.id = mID
        self.message = mMessage
    
    def __str__(self):
        return "{}: {}: {}".format(self.type, self.id, self.message)
