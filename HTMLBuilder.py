import logging

logger = logging.getLogger()
# Remove after testing
f = logging.Formatter('%(levelname)-8s:%(funcName)-20s %(lineno)-5s:%(message)s')
h = logging.StreamHandler()
h.setFormatter(f)
logger.addHandler(h)
logger.setLevel(logging.DEBUG)

from inspect import getmro
from copy import deepcopy

class HTMLObject(object):

    def __init__(self, permittedAttributes, **kwargs):
        self.__depth = 0
        self.__permittedAttributes = ['id','depth','indent','_set_depth','getID','checkAttributes'] + permittedAttributes
        
        for k in kwargs:
            setattr(self, k, kwargs[k])
        
        logging.debug('{} created'.format(self.__class__.__name__))
    
    def __getattribute__(self, name):
        # Pass all inbuilt functions and private attributes up to super()
        if '__' in name:        return super().__getattribute__(name)
        
        # Exclude anything thats not in the permitted list
        if name not in self.__permittedAttributes:
            raise AttributeError('Attribute {} not supported by object {}'.format(name, self.__class__.__name__))

        return super().__getattribute__(name)
    

    def __setattr__(self, name, value):
        if '__' in name:
            super().__setattr__(name, value)
            return
        
        if name not in self.__permittedAttributes:
            raise AttributeError('Attribute {} not supported by object {}'.format(name, self.__class__.__name__))
        
        if name not in attributeTypes:
            raise RuntimeError('Type for attribute {} not defined'.format(name))

        # If an object is passed, clone it first
        if KMLObject in getmro(value.__class__):
            value = deepcopy(value)

        # Check if value is already of the correct type
        if type(value) is attributeTypes[name]:
            super().__setattr__(name, value)
            #logger.debug('Attribute {} of type {} appended to {}'.format(name, type(value).__name__, self.__class__.__name__))                          
            if hasattr(value, 'depth'):
                value.depth = self.__depth + 1
            return

        # Check for derived types
        p = [x for x in getmro(value.__class__) if x.__module__ == self.__class__.__module__ and x.__name__ != 'KMLObject']
        for o in p:
            if o in getmro(attributeTypes[name]):
                super().__setattr__(name, value)
                logger.debug('Attribute {} of type {} appended to {}'.format(name, type(value).__name__, self.__class__.__name__))                          
                if hasattr(value, 'depth'):
                    value.depth = self.__depth + 1
                return
        
        # See if the attribute expects an Enum
        if Enum in getmro(attributeTypes[name]):
            logger.debug('Attribute {} of type {} appended to {}'.format(name, type(attributeTypes[name]).__name__, self.__class__.__name__))                          
            if number.isInt(value):
                super().__setattr__(name, attributeTypes[name](int(value)))    # Set Enum by integer value                        
            else: 
                super().__setattr__(name, attributeTypes[name][value])         # Set the enum by name
            return
        
        # Check for object of incorrect type
        if KMLObject in getmro(value.__class__):
            if type(value) is not attributeTypes[name]:
                raise TypeError('Incorrect object type for {}.  Expected {}, given {}'.format(name, attributeTypes[name].__name__, value.__class__.__name__))
            
        # if we make it this far, then create a new instance using value as an init argument
        tmpobj = attributeTypes[name](value)
        logger.debug('Attribute {} of type {} appended to {}'.format(name, tmpobj.__class__.__name__, self.__class__.__name__))                          
        if hasattr(tmpobj, 'depth'):
            tmpobj.depth = self.__depth + 1
        super().__setattr__(name, tmpobj)                                 # Create an instance of the type passing the value to the init
    
    def __str__(self):
        tmp = ''
        for a in self.__permittedAttributes:        # Cycle through the attributes in order
            if a == 'id': continue
            if a in self.__dict__:
                if self.__dict__[a] is not None:
                    if KMLObject in getmro(self.__dict__[a].__class__):              # Output the attribute if it has been set
                        # Objects handle their own code formatting and indentation
                        tmp += str(self.__dict__[a])
                    else:
                        # Simple attributes can be easily formatted
                        # All enums should know how to return the proper value when requested.  See the __str__() of the respective enum.
                        tmp += self.indent + ' <{}>{}</{}>\n'.format(a,self.__dict__[a],a)
        return tmp
    
    @property
    def indent(self):
        return ' ' * self.__depth

    @property    
    def depth(self, value = None):
        return self.__depth
    
    @depth.setter
    def depth(self, value):
        #self._set_depth(value)
        self.__depth = value
        for a in self.__dict__:
            if hasattr(self.__dict__[a], 'depth'):
                setattr(self.__dict__[a], 'depth', self.__depth + 1)
    """
    def _set_depth(self, value):
        self.__depth = value
        for a in self.__dict__:
            if hasattr(self.__dict__[a], 'depth'):
                setattr(self.__dict__[a], 'depth', self.__depth + 1)
    """
    @property
    def getID(self):
        if 'id' in self.__dict__:
            return ' id="{}"'.format(self.__dict__['id'])
        else:
            return ''
    
    def checkAttributes(self, attributes):
        attr = True
        for a in attributes:
            if a not in self.__dict__:
                logger.warning('{} has missing required attribute {}. No tag returned.'.format(self.__class__.__name__, a))
                attr = False
        return attr

class HTML(HTMLObject):
    
    def __init__(self, attributes, **kwargs):
        self.__permittedAttributes = ['version', 'head', 'body']
        super,__init__(self.__permittedAttributes, **kwargs)
    
    def __str__(self):
        if not self.checkAttributes(['head']):
            return ''
        
        tmp = '<HTML'
        
        



attributeTypes = {
    # Attribute Name                : Data type
    id                              : str,
    depth                           : int,
    
    }

