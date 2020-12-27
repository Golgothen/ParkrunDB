import logging

logger = logging.getLogger()
# Remove after testing
f = logging.Formatter('%(levelname)-8s:%(funcName)-20s %(lineno)-5s:%(message)s')
h = logging.StreamHandler()
h.setFormatter(f)
logger.addHandler(h)
logger.setLevel(logging.DEBUG)

from datetime import datetime
from time import time
from inspect import getmro
from enum import Enum, EnumMeta
from collections import deque
from copy import deepcopy

################################################################################################
#                                                                                              #
#   KML Data Type definitions                                                                  #
#                                                                                              #
################################################################################################

class number(float):
    """
    Subclass of the float data type.
    
    Performs validation on assignment.  Accepts Float and Int.
    
    Provides two class methods:
    
        isInt() - returns true or false if a given argument can be converted to an int
        
        isFloat() - returns true or false if a given argument can be converted to a float
    
    """
    #
    # Extends      : float
    #
    # Extended by  : angle, angle90, angle180, angle360
    #
    # Contains     :
    #
    # Contained By : LabelStyle, IconStyle, Coords, LookAtCoords
    # 
    def __new__(self, value):
        if not number.isFloat(value):
            #if type(value) not in [float, int]:
            raise TypeError('Value must be a number, not {}'.format(value.__class__.__name__))
        return float.__new__(self, value)

    @classmethod
    def isInt(number, value):
        try:
            int(str(value))
            return True
        except ValueError:
            return False

    @classmethod
    def isFloat(number, value):
        try:
            float(str(value))
            return True
        except ValueError:
            return False

class colorAttribute(int):
    """
    Represents a single color attribute.
    
    Syntax:
    
        x = colorAttribute(value)
    
    Args:
    
        value           : (int or hex) Integer value between 0 and 255
                                       HEX value between '00' and 'FF'

    Errors:
    
        TypeError       : value is not int or str
        ValueError      : value is below 0 or above 255
        ValueError      : Invalid literal
    """
    #
    # Extends      : int
    #
    # Extended by  :
    #
    # Contains     :
    #
    # Contained By : Color
    # 

    def __new__(self, value=0):
        if type(value) not in [int, str]:
            raise TypeError('Value must be of type int or str, not {}'.format(value.__class__.__name__))
        if type(value) is str:
            value = int(value, 16)
        if not 0 <= value <= 255:
            raise ValueError('Value must be between 0 and 255')
        return int.__new__(self, value)

    def __str__(self):
        return '{:02X}'.format(self)

class numberPercent(number):
    """
    Represents a percentage value between 0.0 and 1.0

    Syntax:
    
        x = NumberPercent(value)
    
    Args:
    
        value           : (float) Float value between 0.0 and 1.0
    
    Errors:
    
        ValueError      : value is below 0.0 or above 1.0
    """

    #
    # Extends      : number
    #
    # Extended by  :
    #
    # Contains     :
    #
    # Contained By : LineStyle,
    # 

    def __init__(self, value):
        if not (0.0 <= value <= 1.0):
            raise ValueError('Value out of range')

class angle90(number):
    """
    Represents an angle from -90.0 and below 90.0.
    
    Syntax:
    
        x = angle90(value)
    
    Args:
    
        value           : (float) Float value from -90.0 and below 90.0

    Errors:
    
        TypeError       : value is not int or float
        ValueError      : value is below -90.0 or not below 90.0
    """
    def __init__(self, value):
        if not (-90.0 <= value < 90.0):
            raise ValueError('Value out of range')

class anglepos90(number):
    """
    Represents an angle from 0.0 and 90.0.
    
    Syntax:
    
        x = anglepos90(value)
    
    Args:
    
        value           : (float) Float value from 0.0 and 90.0

    Errors:
    
        TypeError       : value is not int or float
        ValueError      : value is below 0.0 or above 90.0
    """
    def __init__(self, value):
        if not (0.0 <= value <= 90.0):
            raise ValueError('Value out of range')

class angle180(number):
    """
    Represents an angle from -180.0 and below 180.0.
    
    Syntax:
    
        x = angle180(value)
    
    Args:
    
        value           : (float) Float value from -180.0 and below 180.0

    Errors:
    
        TypeError       : value is not int or float
        ValueError      : value is below -180.0 or not below 180.0
    """
    def __init__(self, value):
        if not (-180.0 <= value < 180.0):
            raise ValueError('Value out of range')

class angle360(number):
    """
    Represents an angle from 0.0 and below 360.0.
    
    Syntax:
    
        x = angle360(value)
    
    Args:
    
        value           : (float) Float value from 0.0 and below 360.0

    Errors:
    
        TypeError       : value is not int or float
        ValueError      : value is below 0.0 or not below 360.0
    """
    def __init__(self, value):
        if not (0.0 <= value < 360.0):
            raise ValueError('Value out of range')

class Color(object):
    """
    Represents a color as a complete attribute with alpha, red, green and blue.
    
    Color attribute can be expressed as an 8 byte value in the format of aabbggrr:
    
        aa = Alpha
        bb = Blue
        gg = Green
        rr = Red

    Syntax:
    
        x = Color('FF0000FF')                # gives RED
        x = Color(255, 0, 0, 255')           # gives RED
        x = Color('FF', '00', '00', 'FF')    # gives RED
    
    Color can also be expressed as four integer or hex values, in the order of a, b, g, r:
    
    All attributes can be modified using decimal values or hex strings.
    
        x.red = 255
        x.green = 'A0'
        x.alpha = '80'
    
    Errors:
    
        RuntimeError    : Incorrect number of arguments
        ValueError      : Incorrect length of string color argument
    """
    #
    # Extends      :
    #
    # Extended by  :
    #
    # Contains     : colorAttribute
    #
    # Contained by : ColorStyle
    #

    def __init__(self, *args):
        
        self.__alpha = colorAttribute(255)
        self.__red = colorAttribute(255)
        self.__green = colorAttribute(255)
        self.__blue = colorAttribute(255)
        self.__setup(*args)
        logging.debug('Color created')

    def __setup(self, *args):
        if len(args) == 1:                  # If there is only one argument, assume it is a color code
            if type(args[0]) is str:        # Check it's type
                if len(args[0]) != 8:       # check it's length
                    raise ValueError('Color value must be 8 bytes in length, aabbggrr. See help(Color).')
                else:
                    self.color = args[0]    # Assign it to the setter to interpret
        elif len(args) == 4:                # If there are 4 arguments, assume they are color attribute assignments
            self.alpha = args[0]
            self.blue = args[1]
            self.green = args[2]
            self.red = args[3]
        else:
            raise RuntimeError('Invalid number of arguments for Color.  See help(Color) for usage.')
    
    @property
    def color(selfself):
        return self.__str__()
    
    @color.setter
    def color(self, value):
        self.__alpha = colorAttribute(value[:2])
        self.__blue = colorAttribute(value[2:4])
        self.__green = colorAttribute(value[4:6])
        self.__red = colorAttribute(value[6:8])
    
    @property
    def alpha(self):
        return self.__alpha

    @alpha.setter
    def alpha(self, value):
        self.__alpha = colorAttribute(value)

    @property
    def red(self):
        return self.__red

    @red.setter
    def red(self, value):
        self.__red = colorAttribute(value)

    @property
    def green(self):
        return self.__green

    @green.setter
    def green(self, value):
        self.__green = colorAttribute(value)

    @property
    def blue(self):
        return self.__blue

    @blue.setter
    def blue(self, value):
        self.__blue = colorAttribute(value)

    def __str__(self):
        return str(self.__alpha) + str(self.__blue) + str(self.__green) + str(self.__red)

class KMLDateTime(object):
    """
    Date/Time object for handling Date.Time formatting for TimeSpan and TimeStamp
    
    Usage:
    
        x = KMLDateTime('2006-05-01', 'gDate')                  # returns '2006-05-01'
        x = KMLDateTime('2006-05-01', 'gYearMonth')             # returns '2006-05'
        x = KMLDateTime('2006-05-01', 'gYear')                  # returns '2006'
        x = KMLDateTime('2006', 'gDate')                        # returns '2006-01-01'
        x = KMLDateTime('2006', 'gYearMonth')                   # returns '2006-01'
        x = KMLDateTime('2006-05-01T23:59:59Z', 'gDateTime')    # returns '2006-05-01T23:59:59Z'
        x = KMLDateTime('2006-05-01T23:59:59Z', 'UTC')          # returns '2006-05-01T23:59:59+10:00' (local time zone)
        x = KMLDateTime('2006', 'UTC')                          # returns '2006-01-01T00:00:00+10:00' (local time zone)
    """
    #
    # Extends      :
    #
    # Extended by  :
    #
    # Contains     :
    #
    # Contained By : TimeSpan, TimeStamp
    #

    def __init__(self, value=datetime.now()):
        self.__value = None
        self.__format = None
        self.value = value
        #self.__precision = 4

    @property
    def value(self):
        """
        Returns the portion of Date/Time as specified in format.
        
        When setting a value: 
            if only the year is given, the date will default to Jan 01.
            if only the year and month is given, the date will default to the 1st.
        
        
        Time strings must conform to KML standard as per:
            https://developers.google.com/kml/documentation/kmlreference#timestamp
            
        """
        if self.__format in ['gYear']:
            return '{:04}'.format(self.__value.year)
        if self.__format in ['gYearMonth']:
            return '{:04}-{:02}'.format(self.__value.year, self.__value.month)
        if self.__format in ['date']:
            return '{:04}-{:02}-{:02}'.format(self.__value.year, self.__value.month, self.__value.day)
        if self.__format in ['dateTime']:  
            return self.__value.isoformat().split('.')[0] + 'Z'
        if self.__format in ['UTC']:  # TODO: Is there a name for this format in the XML Schema?
            d = datetime.now() - datetime.utcnow()
            t = d.seconds + round(d.microseconds / 1000000)
            return '{}{:+03}:{:02}'.format(self.__value.isoformat().split('.')[0], divmod(t, 3600)[0], divmod(t, 3600)[1])

    @value.setter
    def value(self, value):
        if value is not None:
            if type(value) is datetime:
                self.__value = value
                self.__format = 'dateTime'
                return
            if type(value) is str:
                self.format = self.__getFormat(value)
                # Convert strings to date based in value of format
                if self.format == 'gYear':
                    # Check the Year to make sure it is are valid numbers.  If so, reasemble and convert
                    if number.isInt(value.split('-')[0]):
                        # NOTE: XML Schema 2.0 allows for 5 digit years.  Although this code will manipulate a 5 digit year,
                        #       strptime does not support it so we are stuck with a 4 digit year for now.
                        value = datetime.strptime('{:04}'.format(int(value.split('-')[0])) + '0101', '%Y%m%d')
                    else:
                        raise ValueError('Invalid year {}'.format(value[:4]))
                elif self.format == 'gYearMonth':
                    # Check the Year and Month part to make sure they are valid numbers.  If so, reasemble and convert. Default to 1st of the month
                    dateparts = value[:7].split('-')
                    if number.isInt(dateparts[0]) and number.isInt(dateparts[1]):
                        value = datetime.strptime(dateparts[0] + dateparts[1] + '01', '%Y%m%d')
                    else:
                        raise ValueError('Invalid year-month {}'.format(value[:7]))
                elif self.format == 'date':
                    # Check each Year, Day and Month part to make sure they are valid numbers.  If so, reasemble and convert
                    dateparts = value[:10].split('-')
                    if number.isInt(dateparts[0]) and number.isInt(dateparts[1]) and number.isInt(dateparts[2]):
                        value = datetime.strptime(dateparts[0] + dateparts[1] + dateparts[2], '%Y%m%d')
                    else:
                        raise ValueError('Invalid year-month-day {}'.format(value[:10]))
                elif self.format ==  'dateTime':
                    # Provide strptime the formatting and convert. Straight forward
                    try:
                        value = datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')
                    except:
                        raise  # Raise what ever error gets thrown if it doesnt work
                elif self.format == 'UTC':
                    # Strip the ':' out of the timezone part and use strptime to convert the string to a datetime
                    try:
                        value = datetime.strptime(value[:len(value) - 6] + value[len(value) - 6:].replace(':', ''), '%Y-%m-%dT%H:%M:%S%z')
                    except:
                        raise  # Raise what ever error gets thrown if it doesnt work
            if type(value) is not datetime:
                raise TypeError('Failed to convert {} into Datetime'.format(type(value)))
        self.__value = value

    @property
    def format(self):
        return self.__format
    
    @format.setter
    def format(self, value):
        if value not in ['gYear',             # Year only
                         'gYearMonth',        # Year and Month
                         'date',              # Year, Month and Day
                         'dateTime',          # Full Date/Time UTC
                         'UTC']:              # Full Date/Time with UTC conversion
            raise ValueError('Format pattern does not match')
        self.__format = value
    
    def __getFormat(self, value):
        # Attempt to determine how much of the date we were given and adjust the format appropiately
        if value is None:
            return None
        datepart = None
        timepart = None
        if 'T' in value:
            datepart, timepart = value.split('T')
        else:
            datepart = value
        
        if timepart is None:
            if len(datepart) == 4:                                     # Assume just the year is given
                return 'gYear'
            elif 6 <= len(datepart) <= 7:                                # Assume year and month in either '2004-06' or '200406'
                return 'gYearMonth'
            elif 8 <= len(datepart) <= 10:                               # Assume single date in either '2004-06-12' or '20040612'
                return 'date'
        else:
            if 'Z' in timepart:                    # Assume full date time
                return 'dateTime'
            elif '+' in timepart or '-' in timepart:  # Assume UTC with time zone offset
                return 'UTC'
        raise ValueError('format not set and unable to determine format from {}'.format(value)) 
    
    def __str__(self):
        return self.value

################################################################################################
#                                                                                              #
#   KML Enum definitions                                                                       #
#                                                                                              #
################################################################################################

class altitudeModeEnum(Enum):
    """
        clampToGround - (default)
            Indicates to ignore the altitude specification and drape the overlay over
            the terrain.
        relativeToGround
            Interprets the <altitude> as a value in meters above the ground.
        absolute
            Sets the altitude of the overlay relative to sea level, regardless
            of the actual elevation of the terrain beneath the element. For example, if
            you set the altitude of an overlay to 10 meters with an absolute altitude
            mode, the overlay will appear to be at ground level if the terrain beneath
            is also 10 meters above sea level. If the terrain is 3 meters above sea
            level, the overlay will appear elevated above the terrain by 7 meters.
        relativeToSeaFloor
            Interprets the <altitude> as a value in meters above the sea floor. If the
            point is above land rather than sea, the <altitude> will be interpreted as
            being above the ground.
        clampToSeaFloor
            The <altitude> specification is ignored, and the overlay will be draped over
            the sea floor. If the point is on land rather than at sea, the overlay will
            be positioned on the ground.
    """
    clampToGround = 0
    relativeToGround = 1
    absolute = 2
    relativeToSeaFloor = 3
    clampToSeaFloor = 4
    
    #def __str__(self):
        
class booleanEnum(Enum):
    no = 0
    off = 0
    false = 0
    yes = 1
    on = 1
    true = 1
    
    @classmethod
    def default(cls):
        return cls.yes

    def __str__(self):
        return str(self.value)
    
    def __bool__(self):
        if self.value == 1: return True
        return False

class colorModeEnum(Enum):
    normal = 0
    random = 1
    
    def __str__(self):
        return self.name

class displayModeEnum(Enum):
    default = 0
    hide = 1
    
    def __str__(self):
        return self.name

class hotspotUnitsEnum(Enum):
    fraction = 0
    pixels = 1
    insetPixels = 2
    
    def __str__(self):
        return self.name

class styleMapEnum(Enum):
    normal = 0
    highlighted = 1
    
    def __str__(self):
        return self.name

class listItemTypeEnum(Enum):
    check = 0
    checkOffOnly = 1
    checkHiddenChildren = 2
    radioFolder = 3
    
    def __str__(self):
        return self.name

class itemIconModeEnum(Enum):
    open = 0
    closed = 1
    error = 2
    fetching0 = 3
    fetching1 = 4
    fetching2 = 5
    
    def __str__(self):
        return self.name

class viewerOptionEnum(Enum):
    streetview = 0
    historicalimagery = 1
    sunlight = 2
    
    def __str__(self):
        return self.name

class refreshModeEnum(Enum):
    onChange = 0
    onInterval = 1
    onExpire = 2
    
    def __str__(self):
        return self.name

class schemaTypeEnum(Enum):
    string = 0
    float = 1
    int = 2
    double = 3
    uint = 4
    short = 5
    ushort = 6
    bool = 7
    str = 0
    booleanEnum = 7
    
    def __str__(self):
        return self.name

class viewRefreshModeEnum(Enum):
    never = 0
    onStop = 1
    onRequest = 2
    onRegion = 3
    
    def __str__(self):
        return self.name

class gridOriginEnum(Enum):
    lowerLeft = 0
    upperLeft = 1
    
    def __str__(self):
        return self.name

class shapeEnum(Enum):
    rectangle = 0
    cylinder = 1
    sphere = 2
    
    def __str__(self):
        return self.name

class flyToModeEnum(Enum):
    smooth = 0
    bounce = 1
    
    def __str__(self):
        return self.name

class playModeEnum(Enum):
    pause = 0
    
    def __str__(self):
        return self.name


################################################################################################
#                                                                                              #
#   KML Abstract Object definitions (base classes)                                             #
#                                                                                              #
################################################################################################

class KMLObject(object):

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
                        if a[:3] == 'gx_':
                            tmp += self.indent + ' <{}>{}</{}>\n'.format(a.replace('_',':'),self.__dict__[a],a.replace('_',':'))
                        else:
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
        self._set_depth(value)
    
    def _set_depth(self, value):
        self.__depth = value
        for a in self.__dict__:
            if hasattr(self.__dict__[a], 'depth'):
                setattr(self.__dict__[a], 'depth', self.__depth + 1)

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
        
class altitudeMode(KMLObject):
    # altitudeMode is slightly different as its the only enum that alters its tag based on its value
    # Because of this, it behaves like an object rather than an attribute.
    def __init__(self, value):
        super().__init__(['altitudeModeEnum'])
        self.altitudeModeEnum = altitudeModeEnum[value]
    
    def __str__(self):
        if self.altitudeModeEnum.value < 3:
            return self.indent + '<altitudeMode>{}</altitudeMode>\n'.format(self.altitudeModeEnum.name)
        else:
            return self.indent + '<gx:altitudeMode>{}</gx:altitudeMode>\n'.format(self.altitudeModeEnum.name)
        
class Orientation(KMLObject):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['heading', 'tilt', 'roll']
        super().__init__(self.__permittedAttributes, **kwargs)

    def __str__(self):
        tmp = self.indent + '<Orientation>\n'
        tmp += super().__str__()
        tmp += self.indent + '</Orientation>\n'
        return tmp
    
class Location(KMLObject):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['longitude', 'latitude', 'altitude']
        super().__init__(self.__permittedAttributes, **kwargs)

    def __str__(self):
        tmp = self.indent + '<Location>\n'
        tmp += super().__str__()
        tmp += self.indent + '</Location>\n'
        return tmp
    
class KMLFeature(KMLObject):
    def __init__(self, permittedAttributes, **kwargs):
        self.__permittedAttributes = permittedAttributes + ['name', 'description', 'visibility', 'open', 'atom_link', 
                                      'atom_author', 'address', 'xal_AddressDetails', 'phoneNumber',
                                      'Snippet', 'time', 'view', 'styleUrl', 'styleSelector',
                                      'region', 'extendedData']

        super().__init__(self.__permittedAttributes, **kwargs)
        
    def __str__(self):
        return super().__str__()

class ATOMLink(KMLObject):
    # atom:link is a special case.  It has the link value inside the tag.  No other attributes permitted
    def __init__(self, value):
        # String value must be given on creation
        super().__init__(['value'])
        self.value = value
    
    def __str__(self):
        return self.indent + '<atom:link href="{}" />\n'.format(self.value)
    
class ATOMAuthor(KMLObject):
    def __init__(self, name):
        # String value must be given on creation
        super().__init__(['name'])
        self.name = name
    
    def __str__(self):
        tmp = self.indent + '<atom:author>\n'
        tmp += super().__str__()
        tmp += self.indent + '</atom:author>\n'
        return tmp

class XALAddress(KMLObject):
    def __init__(self, address):
        # String value must be given on creation
        super().__init__(['adress'])
        self.address = address
    
    def __str__(self):
        return self.indent + '<xal:AddressDetails>{}</xal:AddressDetails>\n'.format(self.address)

class Snippet(KMLObject):
    def __init__(self, **kwargs):
        super().__init__(['text','maxLines'], **kwargs)
        
    def __str__(self):
        if not self.checkAttributes(['text']):
            return ''
        else:
            tmp = self.indent + '<Snippet'
            if 'maxLines' in self.__dict__:
                tmp += ' maxLines="{}"'.format(self.maxLines)
            tmp += '>{}</Snippet>\n'.format(self.text)
            return tmp
        
class TimePrimitive(KMLObject):
    """
    Abstract class that returns a TimeStamp or TimeSpan object depending on the number of values given.
    
    If one value is given, a TimeStamp object is returned.
    If two (or more) values are given, a TimeSpan object is returned.  The first value will be 'begin' and
    the second value will be 'end'
    
    Options can be passed as a string (for one option), list or tuple (for two or more)
    
    Only the first two values will be used.  Any other values in the list or tuple will be ignored.
    
    All values must be valid DateTime values.
    
    Usage:
    
        x = TimePrimitive('2006')             # returns TimeStamp with <when> = '2006'
        x = TimePrimitive(['2006','2007'])    # returns TimeSpan with <begin> = '2006' and <end> = '2007'
        x = TimePrimitive(['2006',None])      # returns TimeSpan with <begin> = '2006' and no <end>
    
        f = KMLFeature()
        f.time = '2006'                       # f.time is TimeStamp with <when> = '2006'
        f.time = '2006', '2007'               # f.time is TimeSpan with <begin> = '2006' and <end> = '2007'
    """
    def __new__(self, arg, gx = False):
        if type(arg) in [TimeSpan, TimeStamp, GXTimeSpan, GXTimeStamp]:
            return arg
        if type(arg) is str:
            if gx:
                return GXTimeStamp(arg)
            else:
                return TimeStamp(arg)
        if type(arg) is list:
            arg = tuple(arg)
        if len(arg) == 1:
            if gx:
                return GXTimeStamp(arg[0])
            else:
                return TimeStamp(arg[0])
        if len(arg) >= 2:
            if gx:
                return GXTimeSpan(arg[0], arg[1])
            else:
                return TimeSpan(arg[0], arg[1])

class TimeStamp(KMLObject):
    def __init__(self, value):
        super().__init__(['when'])
        self.when = value
    
    def __str__(self):
        tmp = self.indent + '<TimeStamp{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</TimeStamp>\n'
        return tmp

class GXTimeStamp(KMLObject):
    def __init__(self, value):
        super().__init__(['when'])
        self.when = value
    
    def __str__(self):
        tmp = self.indent + '<GXTimeStamp{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</GXTimeStamp>\n'
        return tmp
        
class TimeSpan(KMLObject):
    def __init__(self, begin = None, end = None):
        super().__init__(['begin', 'end'])
        if begin is not None:
            self.begin = begin
        if end is not None:
            self.end = end
    
    def __str__(self):
        tmp = self.indent + '<TimeSpan{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</TimeSpan>\n'
        return tmp

class GXTimeSpan(KMLObject):
    def __init__(self, begin = None, end = None):
        super().__init__(['begin', 'end'])
        if begin is not None:
            self.begin = begin
        if end is not None:
            self.end = end
    
    def __str__(self):
        tmp = self.indent + '<GXTimeSpan{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</GXTimeSpan>\n'
        return tmp

class Coordinate(KMLObject):
    """
    Coordinate object.
    
    Syntax:
    
        x = Coordinate(lon, lat, [alt])
    
    Args:
    
        lon             : (angle180) Longitude
        lat             : (angle90) Latitude
        alt             : (number) Altitude (optional)
        
    """
    def __init__(self, **kwargs):
        super().__init__(['longitude', 'latitude', 'altitude'], **kwargs)

    def __str__(self):
        if not self.checkAttributes(['latitude','longitude']):
            return ''
        else:
            if 'altitude' in self.__dict__:
                return '{},{},{}'.format(self.longitude, self.latitude, self.altitude)
            else:
                return '{},{}'.format(self.longitude, self.latitude)

    def __eq__(self, x):
        if type(x) != type(self):         return False
        if self.latitude != x.latitude:   return FalSe
        if self.longitude != x.longitude: return FalSe
        if hasattr(self, 'altitude') and hasattr(x, 'altitude'):
            if self.altitude != x.altitude : return False
        return True
           
class Container(KMLObject, deque):
    """
    Creates a collection of objects.
    
    Usage:
    
        x = Container(attributes, types, [unique])
        
    Args:
    
        attributes      : (list) List of additional attributes permitted on this object
        types           : (list) List of data types permitted in this collection.
                                 Parse an empty list for no restrictions.
        unique          : (bool) Control if duplicate objects are permitted.
                                 Objects are compared via == (__eq__) method.
                                 Optional - default is False (duplicates permitted)
    """
    
    def __init__(self, attributes, types, unique, **kwargs):
        
        
        # Add the class attributes we need for the deque object
        super().__init__(attributes + ['append', 'appendleft', 'clear', 'count', 'index',
                                       'insert', 'pop', 'popleft', 'reverse', 'rotate'], **kwargs)
        # List of data types permitted in this collection
        self.__restrictTypes = False
        if len(types) > 0:
            self.__validTypes = types
            self.__restrictTypes = True
        # Flag to permit duplicate entries.  Objects should override __eq__ to control comparison between objects
        self.__unique = unique
    
    def __deepcopy__(self, memo):
        obj = type(self)()
        obj.__dict__.update(self.__dict__)
        for o in range(len(self)):
            obj.append(deepcopy(self[o]))
        return obj
        
    def __validateType(self, value):

        # Allows all "add" methods to validate objects the same way
        
        # Clone the object first
        value = deepcopy(value)

        # Check for valid data types:
        valid = False
        if self.__restrictTypes:
            # Direct type match
            if type(value) in self.__validTypes:
                valid =  True
            # Check for derived types:
            if not valid:
                for t in self.__validTypes:
                    if t in getmro(value.__class__):
                        valid = True
                        break
            # Raise an error on type mismatch
            if not valid:
                raise TypeError('Type {} invalid for container {}'.format(value.__class__.__name__, self.__class__.__name__))
        # Check if duplicates allowed 
        if self.__unique:
            for i in range(len(self)):
                if value == self[i]:
                    # Raise error on duplicates when not allowed
                    raise ValueError('Cannot duplicate {} when container {} unique is True'.format(value.__class__.__name__, self.__class__.__name__))
        # On success, return the value that was passed to us with an increase to depth
        value.depth = self.depth + 1
        return value
                
    def _set_depth(self, value):
        super()._set_depth(value)
        for a in range(len(self)):
            self[a].depth = self.depth + 1
    
    def append(self, value):
        value = self.__validateType(value)
        super().append(value)
        return value

    def appendleft(self, value):
        value = self.__validateType(value)
        super().appendleft(value)
        return value

    def insert(self, index, value):
        value = self.__validateType(value)
        super().insert(index, value)
        return value
    
    def __str__(self):
        tmp = ''
        tmp += super().__str__()
        for i in range(len(self)):
            tmp += str(self[i])
        return tmp

class Coordinates(Container):
    def __init__(self):
        super().__init__([],[Coordinate], False)
    
    def __str__(self):
        if len(self) == 0: return ''
        tmp = self.indent + '<coordinates>'
        if len(self) == 1:
            tmp += str(self[0]) + '</coordinates>\n'
        else:
            tmp += '\n'
            for c in range(len(self)):
                tmp += self.indent + ' ' + str(self[c]) + '\n'
            tmp += self.indent + '</coordinates>\n'
        return tmp

class GXViewerOption(KMLObject):
    """
    Represents a singe gx:ViewerOptions choice entry
    
    Syntax:
    
        x = GXViewerOption(name, enabled)
    
    Args:
    
        name            : (viewerOptionEnum) Option name
        enabled         : (boolEnum) Enabled (optional) default 1
    
    Comparing GXViewerOption will only compare by name, not value. 
    """ 
    def __init__(self, name, value = 1):
        super().__init__(['gx_optionName','enabled'])
        self.gx_optionName = name
        self.enabled = value
    
    def __str__(self):
        return self.indent + '<gx:option name="{}" enabled={}/>\n'.format(self.gx_optionName, self.enabled)
    
    def __eq__(self, x):
        if type(x) is str:
            return self.gx_optionName == x
        else:
            return self.gx_optionName == x.gx_optionName
        
class GXViewerOptions(Container):
    """
    Collection of GXViewerOption objects
    
    Syntax:
    
        x = GXViewerOptions()
        x.append(GXViewerOption(...))
    """
    def __init__(self):
        super().__init__(['seek'], [GXViewerOption], True)

    def __str__(self):
        if len(self) == 0:
            return ''
        tmp = self.indent + '<gx:ViewerOptions>\n'
        tmp += super().__str__()
        tmp += self.indent + '</gx:ViewerOptions>\n'
        return tmp

    def seek(self, item):
        """
        returns reference to a GXViewerOption object by name
        
        Syntax:
        
            x = GXViewerOptions()
            x.seek('streetview')
        
        Can be used to modify attributes:

            x.seek('streetview').enabled = 'yes'
        """
        try:
            return self[self.index(GXViewerOption(item))]
        except ValueError:
            raise ValueError('GXViewerOption {} not found in GXViewerOptions'.format(item))
            
class KMLView(KMLObject):
    # Inheritance placeholder class for all View types
    def __init__(self, permittedAttributes, **kwargs):
        self.__permittedAttributes = ['longitude', 'latitude', 'altitude','altitudeMode', 'time',
                                      'viewerOptions', 'heading', 'tilt'] + permittedAttributes
        super().__init__(self.__permittedAttributes, **kwargs)
        if 'viewerOptions' not in self.__dict__:
            self.viewerOptions = GXViewerOptions()
        
class Camera(KMLView):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['roll']
        super().__init__(self.__permittedAttributes, **kwargs)

    def __str__(self):
        tmp = self.indent + '<Camera{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</Camera>\n'
        return tmp
                
class LookAt(KMLView):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['range']
        super().__init__(self.__permittedAttributes, **kwargs)
                
    def __str__(self):
        tmp = self.indent + '<LookAt{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</LookAt>\n'
        return tmp
        
class Icon(KMLObject):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['href', 'gx_x', 'gx_y', 'gx_w', 'gx_h',
                                      'refreshMode', 'refreshInterval',
                                      'viewBoundScale', 'viewFormat',
                                      'httpQuery'] 
        super().__init__(self.__permittedAttributes, **kwargs)
                
    def __str__(self):
        if not self.checkAttributes(['href']):
            return ''
        else:
            tmp = self.indent + '<Icon{}>\n'.format(self.getID)
            tmp += super().__str__()
            tmp += self.indent + '</Icon>\n'
            return tmp

class Vector(KMLObject):
    def __init__(self, attributes, **kwargs):
        super().__init__(attributes + ['x', 'y', 'xunits', 'yunits'], **kwargs)
    
    def __str__(self):
        if not self.checkAttributes(['x', 'y', 'xunits', 'yunits']):
            return ''
        else:
            return 'x="{}" y="{}" xunits="{}" yunits="{}"'.format(self.x, self.y, self.xunits, self.yunits)

class HotSpot(Vector):
    def __init__(self, **kwargs):
        super().__init__([], **kwargs)
    
    def __str__(self):
        return self.indent + '<hotSpot {}/>\n'.format(super().__str__())
 
class IconStyle(KMLObject):
    def __init__(self, **kwargs):
        
        self.__permittedAttributes = ['color', 'colorMode', 'scale', 'heading', 'icon', 'hotSpot']
        super().__init__(self.__permittedAttributes, **kwargs)

    def __str__(self):
        tmp = self.indent + '<IconStyle{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</IconStyle>\n'
        return tmp

class LabelStyle(KMLObject):
    def __init__(self, **kwargs):
        
        self.__permittedAttributes = ['color', 'colorMode', 'scale']
        super().__init__(self.__permittedAttributes, **kwargs)

    def __str__(self):
        tmp = self.indent + '<LabelStyle{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</LabelStyle>\n'
        return tmp

class LineStyle(KMLObject):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['color', 'colorMode', 'width', 'gx_outerColor', 'gx_outerWidth',
                                      'gx_physicalWidth', 'gx_labelVisibility']
        super().__init__(self.__permittedAttributes, **kwargs)

    def __str__(self):
        tmp = self.indent + '<LineStyle{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</LineStyle>\n'
        return tmp

class PolyStyle(KMLObject):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['color', 'colorMode', 'fill', 'outline']
        super().__init__(self.__permittedAttributes, **kwargs)

    def __str__(self):
        tmp = self.indent + '<PolyStyle{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</PolyStyle>\n'
        return tmp

class BalloonStyle(KMLObject):
    def __init__(self, **kwargs):
        
        self.__permittedAttributes = ['bgColor', 'textColor', 'text', 'displayMode']
        super().__init__(self.__permittedAttributes, **kwargs)

    def __str__(self):
        tmp = self.indent + '<BalloonStyle{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</BalloonStyle>\n'
        return tmp

class ItemIcon(KMLObject):
    def __init__(self, **kwargs):
        super().__init__(['state', 'href'], **kwargs)
    
    def __str__(self):
        if not self.checkAttributes(['state','href']):
            return ''
        else:
            tmp = self.indent + '<ItemIcon>\n'
            tmp += super().__str__()
            tmp += self.indent + '</ItemIcon>\n'
            return tmp
    
    def __eq__(self, x):
        if type(x) is str:
            return self.state == x
        else:
            return self.state == x.state

class ListStyle(Container):
    def __init__(self, **kwargs):
        super().__init__(['listItemType', 'bgColor'],[ItemIcon], True, **kwargs)
        
    def __str__(self):
        if len(self) == 0: return ''
        tmp = self.indent + '<ListStyle{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</ListStyle>\n'
        return tmp

class StyleSelector(KMLObject):
    # Inheritance placeholder class for all geometry types
    def __init__(self, permittedAttributes, **kwargs):
        self.__permittedAttributes = permittedAttributes
        super().__init__(self.__permittedAttributes, **kwargs)

class Style(StyleSelector):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['IconStyle', 'LabelStyle', 'LineStyle',
                                      'PolyStyle', 'BalloonStyle', 'ListStyle']

        super().__init__(self.__permittedAttributes, **kwargs)
        if 'ListStyle' not in self.__dict__:
            self.ListStyle = ListStyle()
                
    def __str__(self):
        tmp = self.indent + '<Style{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</Style>\n'
        return tmp
    
class StyleMapPair(KMLObject):
    """
    Represents a singe StyleMap Key Value pair
    
    Syntax:
    
        x = StyleMapPair(key, styleUrl)
    
    Args:
    
        key             : (viewerOptionEnum) Option name
        styleUrl        : (str) URL to the style
    
    Comparing StyleMapPair will only compare by key, not value. 
    """ 
    def __init__(self, **kwargs):
        super().__init__(['key','styleUrl'], **kwargs)
        
    def __str__(self):
        if not self.checkAttributes(['key','styleUrl']):
            return ''
        else:
            tmp = self.indent + '<Pair>\n'
            tmp += super().__str__()
            tmp += self.indent + '</Pair>\n'
            return tmp
    
    def __eq__(self, x):
        if type(x) is str:
            return self.key == x
        else:
            return self.key == x.key

class StyleMap(Container, StyleSelector):
    def __init__(self):
        super().__init__([], [StyleMapPair], True, **kwargs)
    
    def __str__(self):
        tmp = self.indent + '<StyleMap{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</StileMap>\n'
        return tmp

    def seek(self, item):
        """
        returns reference to a StyleMapPair object by key
        
        Syntax:
        
            x = StyleMap()
            x.seek('normal')
        
        Can be used to modify attributes:

            x.seek('normal').styleUrl = '#MyStyle'
        """
        try:
            return self[self.index(StyleMapPair(item))]
        except ValueError:
            raise ValueError('StyleMapPair {} not found in StyleMap'.format(item))
            
class LatLonAltBox(KMLObject):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['north', 'south', 'east', 'west', 'minAltitude', 'maxAltitude', 'altitudeMode']
        super().__init__(self.__permittedAttributes, **kwargs)

    def __str__(self):
        if not self.checkAttributes(['north', 'south', 'east', 'west']):
            return ''
        else:
            tmp = self.indent + '<LatLonAltBox>\n'
            tmp += super().__str__()
            tmp += self.indent + '</LatLonAltBox>\n'
            return tmp
    
class LatLonBox(KMLObject):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['north', 'south', 'east', 'west', 'rotation']
        super().__init__(self.__permittedAttributes, **kwargs)

    def __str__(self):
        if not self.checkAttributes(['north', 'south', 'east', 'west']):
            return ''
        else:
            tmp = self.indent + '<LatLonBox>\n'
            tmp += super().__str__()
            tmp += self.indent + '</LatLonBox>\n'
            return tmp
    
class LatLonQuad(KMLObject):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['coordinates']
        super().__init__(self.__permittedAttributes, **kwargs)
        if 'coordinates' not in self.__dict__:
            self.coordinates = Coordinates()
        
    def __str__(self):
        if len(self.coordinates) < 4:
            logger.warning('LatLonQuad has less than four coordinate points set. Nothing returned')
            return ''
        else:
            tmp = self.indent + '<gx:LatLonBox>\n'
            tmp += self.indent + ' <coordinates>{} {} {} {}</coordinates>\n'.format(self.coordinates[0],
                                                                                    self.coordinates[1],
                                                                                    self.coordinates[2],
                                                                                    self.coordinates[3])
            tmp += self.indent + '</gx:LatLonBox>\n'
            return tmp
    
class Lod(KMLObject):
    def __init__(self, **kwargs):
        super().__init__(['minLodPixels', 'maxLodPixels', 'minFadeExtent', 'maxFadeExtent'], **kwargs)
    
    def __str__(self):
        if not self.checkAttributes(['minLodPixels']):
            return ''
        else:
            tmp = self.indent + '<Lod>\n'
            tmp += super().__str__()
            tmp += self.indent + '</Lod>\n'
            return tmp
    
class Region(KMLObject):
    def __init__(self, **kwargs):
        super().__init__(['LatLonAltBox', 'Lod'], **kwargs)
        
    def __str__(self):
        if not self.checkAttributes(['LatLonAltBox']):
            return ''
        else:
            tmp = self.indent + '<Region{}>\n'.format(self.getID)
            tmp += super().__str__()
            tmp += self.indent + '</Region>\n'
            return tmp

class SimpleField(KMLObject):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['name', 'type', 'displayName']
        super().__init__(self.__permittedAttributes, **kwargs)
    
    def __str__(self):
        if not self.checkAttributes(['name','type']):
            return ''
        else:
            tmp = self.indent + '<SimpleField type="{}" name="{}">\n'.format(self.type, self.name)
            if 'displayName' in self.__dict__:
                tmp += self.indent + ' <displayName>{}</displayName>\n'.format(self.displayName)
            tmp += self.indent + '</SimpleField>\n'
            return tmp
    
    def __eq__(self, x):
        if type(x) is str:
            return self.name == x
        else:
            return self.name == x.name

class SimpleArrayField(KMLObject):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['name', 'type', 'displayName']
        super().__init__(self.__permittedAttributes, **kwargs)
    
    def __str__(self):
        if not self.checkAttributes(['name','type']):
            return ''
        else:
            tmp = self.indent + '<gx:SimpleArrayField type="{}" name="{}">\n'.format(self.type, self.name)
            if 'displayName' in self.__dict__:
                tmp += self.indent + ' <displayName>{}</displayName>\n'.format(self.displayName)
            tmp += self.indent + '</gx:SimpleArrayField>\n'
            return tmp
    
    def __eq__(self, x):
        if type(x) is str:
            return self.name == x
        else:
            return self.name == x.name

class Schema(Container):
    def __init__(self, **kwargs):
        super().__init__(['name','loadFromCSV','csvFile','csvparams'], [SimpleField, SimpleArrayField], True, **kwargs)

    def __str__(self):
        if not self.checkAttributes(['name','id']):
            return ''
        else:
            tmp = self.indent + '<Schema name="{}"{}>\n'.format(self.name, self.getID)
            for a in range(len(self)):
                tmp += str(self[a])
            tmp += self.indent + '</Schema>\n'
            return tmp
    
    def loadFromCSV(self, file, fieldlist = [], **fmtparams):
        import csv
        with open(file, newline = '') as csvfile:
            reader = csv.reader(csvfile, **fmtparams)
            headings = next(reader)
        if len(fieldlist)>0:
            fields = [x for x in headings if x in fieldlist]
        else:
            fields = headings
        for f in fields:
            self.append(SimpleArrayField(name = f, type = 'string'))
        self.csvFile = file
        self.csvparams = fmtparams
    
class SimpleData(KMLObject):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['name', 'value']
        super().__init__(self.__permittedAttributes, **kwargs)
    
    def __str__(self):
        if not self.checkAttributes(['name','value']):
            return ''
        else:
            return self.indent + '<SimpleData name="{}">{}</SimpleData>\n'.format(self.name, self.value)
    
    def __eq__(self, x):
        if type(x) is str:
            return self.name == x
        else:
            return self.name == x.name

class SchemaData(Container):
    def __init__(self, **kwargs):
        super().__init__(['schema', 'addData', 'loadCSV', 'autoload'], [SimpleData, SimpleArray], True, **kwargs)
        
        # Automatically create SimpleArray objects for each SimpleArrayField defined in the given schema
        if 'schema' in self.__dict__:
            for i in range(len(self.schema)):
                if type(self.schema[i]) is SimpleArrayField:
                    self.append(SimpleArray(name = self.schema[i].name))
        
        if 'autoload' in self.__dict__:
            if self.autoload:
                self.loadData()
    
    def loadCSV(self):
        if 'schema' in self.__dict__:
            if 'csvFile' in self.schema.__dict__:
                import csv
                fields = [x.name for x in self]
                with open(self.schema.csvFile, newline = '') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        for i in range(len(self.schema)):
                            self[i].append(SimpleArrayData(value = row[self[i].name]))
                        
        
    def __str__(self):
        if not self.checkAttributes(['schema']):
            return ''
        else:
            tmp = self.indent + '<SchemaData schemaUrl="#{}">\n'.format(self.schema.id)
            for a in range(len(self)):
                tmp += str(self[a])
            tmp += self.indent + '</SchemaData>\n'
            return tmp

    def addData(self, data):
        #load SchemaData object with data from a dict
        for d in data:
            if d in self.schema:
                self.append(SimpleData(name = d, value = data[d]))

    def append(self, value):
        if value.name in self.schema:
            super().append(value)
        else:
            raise KeyError('Field {} not found in schema {}'.format(value.name, self.schema.id))

    def appendleft(self, value):
        if value.name in self.schema:
            super().appendleft(value)
        else:
            raise KeyError('Field {} not found in schema {}'.format(value.name, self.schema.id))

    def insert(self, index, value):
        if value.name in self.schema:
            super().insert(index, value)
        else:
            raise KeyError('Field {} not found in schema {}'.format(value.name, self.schema.id))
        
class SimpleArrayData(KMLObject):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['value']
        super().__init__(self.__permittedAttributes, **kwargs)
    
    def __str__(self):
        if not self.checkAttributes(['value']):
            return ''
        else:
            return self.indent + '<gx:value>{}</gx:value>\n'.format(self.value)

class SimpleArray(Container):
    def __init__(self, **kwargs):
        super().__init__(['name', 'addData'], [SimpleArrayData], False, **kwargs)
    
    def __str__(self):
        if not self.checkAttributes(['name']):
            return ''
        else:
            tmp = self.indent + '<gx:SimpleArrayData name="{}">\n'.format(self.name)
            for a in range(len(self)):
                tmp += str(self[a])
            tmp += self.indent + '</gx:SimpleArrayData>\n'
            return tmp

    def __eq__(self, x):
        if type(x) is str:
            return self.name == x
        else:
            return self.name == x.name
    
class ExtendedData(KMLObject):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['schemaData']
        super().__init__(self.__permittedAttributes, **kwargs)

    def __str__(self):
        tmp = self.indent + '<ExtendedData>\n'
        tmp += super().__str__()
        tmp += self.indent + '</ExtendedData>\n'
        return tmp

class Link(KMLObject):
    def __init__(self, href, **kwargs):
        self.__permittedAttributes = ['href',
                                      'refreshMode', 'refreshInterval',
                                      'viewRefreshMode', 'viewRefreshTime',
                                      'viewBoundScale', 'viewFormat',
                                      'httpQuery'] 

        super().__init__(self.__permittedAttributes, **kwargs)
                
    def __str__(self):
        tmp = self.indent + '<Link{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</Link>\n'
        return tmp

class Scale(KMLObject):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['x', 'y', 'z']
        super().__init__(self.__permittedAttributes, **kwargs)

    def __str__(self):
        tmp = self.indent + '<Scale>\n'
        tmp += super().__str__()
        tmp += self.indent + '</Scale>\n'
        return tmp

class Alias(KMLObject):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['sourceHref', 'targetHref']
        super().__init__(self.__permittedAttributes, **kwargs)

    def __str__(self):
        if not self.checkAttributes(['sourceHref','targetHref']):
            return ''
        else:
            tmp = self.indent + '<Alias>\n'
            tmp += super().__str__()
            tmp += self.indent + '</Alias>\n'
            return tmp
    
    def __eq__(self, x):
        if self.sourceHref == x.sourceHref and \
           self.targetHref == x.targetHref:
            return True
        return False

class ResourceMap(Container):
    def __init__(self):
        super().__init__([], [Alias], True)
        
    def __str__(self):
        tmp = self.indent + '<ResourceMap>\n'
        for a in range(len(self)):
            tmp += str(self[a])
        tmp += self.indent + '</ResourceMap>\n'
        return tmp
    
class Folder(Container, KMLFeature):
    def __init__(self, **kwargs):
        self.__permittedAttributes = []

        super().__init__(self.__permittedAttributes, [KMLObject], False, **kwargs)

    def __str__(self):
        tmp = self.indent + '<Folder{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '<Folder>\n'
        return tmp

class Document(Container, KMLFeature):
    def __init__(self, **kwargs):
        self.__permittedAttributes = []

        super().__init__(self.__permittedAttributes, [KMLObject], False, **kwargs)
        
    def __str__(self):
        tmp = self.indent + '<Document{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '<Document>\n'
        return tmp

class KMLGeometry(KMLObject):
    # Inheritance placeholder class for all geometry types
    def __init__(self, permittedAttributes, **kwargs):
        self.__permittedAttributes = permittedAttributes
        super().__init__(self.__permittedAttributes, **kwargs)

class Point(KMLGeometry):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['extrude', 'altitudeMode', 'coordinates']
        super().__init__(self.__permittedAttributes, **kwargs)
        if 'coordinates' not in self.__dict__:
            self.coordinates = Coordinates()
        
    def __str__(self):
        tmp = self.indent + '<Point{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</Point>\n'
        return tmp

class LineString(KMLGeometry):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['gx_altitudeOffset', 'extrude', 'tessellate',
                                      'altitudeMode', 'gx_drawOrder', 'coordinates']
        super().__init__(self.__permittedAttributes, **kwargs)
        if 'coordinates' not in self.__dict__:
            self.coordinates = Coordinates()

    def __str__(self):
        tmp = self.indent + '<LineString{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</LineString>\n'
        return tmp

class LinearRing(KMLGeometry):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['gx_altitudeOffset', 'extrude', 'tessellate', 'altitudeMode', 'coordinates']
        super().__init__(self.__permittedAttributes, **kwargs)
        if 'coordinates' not in self.__dict__:
            self.coordinates = Coordinates()

    def __str__(self):
        tmp = self.indent + '<LinearRing{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</LinearRing>\n'
        return tmp

class OuterBoundary(KMLObject):
    def __init__(self, **kwargs):
                
        self.__permittedAttributes = ['linearRing']
        super().__init__(self.__permittedAttributes, **kwargs)

    def __str__(self):
        if not self.checkAttributes(['linearRing']):
            return ''
        else:
            tmp = self.indent + '<outerBoundaryIs>\n'
            tmp += super().__str__()
            tmp += self.indent + '</outerBoundaryIs>\n'
            return tmp

class InnerBoundary(KMLObject):
    def __init__(self, **kwargs):
                
        self.__permittedAttributes = ['linearRing']
        super().__init__(self.__permittedAttributes, **kwargs)

    def __str__(self):
        if not self.checkAttributes(['linearRing']):
            return ''
        else:
            tmp = self.indent + '<innerBoundaryIs>\n'
            tmp += super().__str__()
            tmp += self.indent + '</innerBoundaryIs>\n'
            return tmp

class Polygon(KMLGeometry):
    def __init__(self, **kwargs):
        
        self.__permittedAttributes = ['outerBoundaryIs', 'extrude', 'tessellate',
                                      'altitudeMode', 'innerBoundaryIs']
        super().__init__(self.__permittedAttributes, **kwargs)

    def __str__(self):
        if not self.checkAttributes(['outerBoundaryIs']):
            return ''
        else:
            tmp = self.indent + '<Polygon{}>\n'.format(self.getID)
            tmp += super().__str__()
            tmp += self.indent + '</Polygon>\n'
            return tmp

class MultiGeometry(Container):
    def __init__(self):
        self.__permittedAttributes = []

        super().__init__(self.__permittedAttributes, [KMLGeometry], False)
        
    def __str__(self):
        tmp = self.indent + '<MultiGeometry{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '<MultiGeometry>\n'
        return tmp
    
class Model(KMLGeometry):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['altitudeMode', 'location', 'orientation', 'modelScale',
                                      'link', 'resourceMap']
        super().__init__(self.__permittedAttributes, **kwargs)

        self.resourceMap = ResourceMap()

    def __str__(self):
        tmp = self.indent + '<Model{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</Model>\n'
        return tmp

class GXCoord(KMLObject):
    def __init__(self, **kwargs):
        
        self.__permittedAttributes = ['latitude', 'longitude', 'altitude']
        super().__init__(self.__permittedAttributes)
        for k in kwargs:
            if k in self.__permittedAttributes:
                if kwargs[k] is not None:
                    setattr(self, k, kwargs[k])

    def __str__(self):
        tmp = '<gx:coord>'
        if 'longitude' in self.__dict__ and \
           'latitude' in self.__dict__:
            if self.longitude is not None and \
               self.latitude is not None:
                tmp += '{} {}'.format(self.longitude, self.latitude)
                if 'altitude' in self.__dict__:
                    tmp += ' {}'.format(self.altitude)
        tmp += '</gx:coord>\n'
        return tmp

class GXAngle(KMLObject):
    def __init__(self, **kwargs):
        
        self.__permittedAttributes = ['heading', 'tilt', 'roll']
        super().__init__(self.__permittedAttributes)
        for k in kwargs:
            if k in self.__permittedAttributes:
                if kwargs[k] is not None:
                    setattr(self, k, kwargs[k])

    def __str__(self):
        tmp = '<gx:angles>'
        if 'heading' in self.__dict__:
            tmp += '{}'.format(self.heading)
        if 'tilt' in self.__dict__:
            tmp += ' {}'.format(self.tilt)
        if 'roll' in self.__dict__:
            tmp += ' {}'.format(self.roll)
        tmp += '</gx:angles>\n'
        return tmp

class TrackTimes(Container):
    def __init__(self):
        self.__permittedAttributes = []
        super().__init__(self.__permittedAttributes, [KMLDateTime], False)
    
    def __str__(self):
        tmp = ''
        for i in range(len(self)):
            tmp += self.indent + '<when>{}</when>\n'.format(self[i])
        return tmp
        
class TrackCoords(Container):
    def __init__(self):
        self.__permittedAttributes = []
        super().__init__(self.__permittedAttributes, [GXCoord], False)
    
    def __str__(self):
        tmp = ''
        for i in range(len(self)):
            tmp += self.indent + str(self[i])
        return tmp
        
class TrackAngles(Container):
    def __init__(self):
        self.__permittedAttributes = []
        super().__init__(self.__permittedAttributes, [GXAngle], False)
    
    def __str__(self):
        tmp = ''
        for i in range(len(self)):
            tmp += self.indent + str(self[i])
        return tmp
        
class Track(KMLGeometry):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['extendedData', 'schema', 'times', 'timeField', 'csvFile',
                                      'timeFormat', 'coords', 'coordFields', 'loadCSV', 'autoload',
                                      'angles', 'angleFields', 'altitudeMode', 'model']
        super().__init__(self.__permittedAttributes, **kwargs)

        self.times = TrackTimes()
        self.coords = TrackCoords()
        self.angles = TrackAngles()
        
        if 'autoload' in self.__dict__:
            if self.autoload:
                self.loadCSV()
    
    def loadCSV(self):
        if 'timeField' not in self.__dict__:
            raise ValueError('Time field must be set')
        if 'coordFields' not in self.__dict__:
            raise ValueError('Coord fields must be set')
        if 'schema' not in self.__dict__:
            raise RuntimeError('Cannot loadCSV(). No schema set')
        
        if 'csvFile' not in self.__dict__:
            if 'csvFile' in self.schema.__dict__:
                self.csvFile = self.schema.csvFile
            else:
                raise RuntimeError('Cannot loadCSV(). No CSV file set')
        
        if 'extendedData' not in self.__dict__:
            raise ValueError('Track has no data. Nothing to output')
        
        self.extendedData = ExtendedData(schemaData = SchemaData(schema = self.schema, autoload = False))
        # Delete time and coord data from the SchemaData as they are stored in the track
        if self.timeField in self.extendedData.schemaData:
            del self.extendedData.schemaData[self.extendedData.schemaData.index(self.timeField)]
        if 'coordFields' not in self.__dict__:
            raise RuntimeError('Cannot loadCSV(). No coord fields set')
        if len(self.coordFields) < 2:
            raise RuntimeError('Cannot loadCSV(). Coords needs at least longitude and latitude')
        
        for c in self.coordFields:
            if c in self.extendedData.schemaData:
                del self.extendedData.schemaData[self.extendedData.schemaData.index(c)]
        if 'angleFields' in self.__dict__:
            for c in self.angleFields:
                if c in self.extendedData.schemaData:
                    del self.extendedData.schemaData[self.extendedData.schemaData.index(c)]
        
        import csv
        with open(self.csvFile, newline = '') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                for i in range(len(self.extendedData.schemaData)):
                    self.extendedData.schemaData[i].append(SimpleArrayData(value = row[self.extendedData.schemaData[i].name]))
                if 'timeFormat' in self.__dict__:
                    self.times.append(KMLDateTime(datetime.strptime(row[self.timeField], self.timeFormat)))
                else:
                    self.times.append(KMLDateTime(row[self.timeField]))
                lon = None if not number.isFloat(row[self.coordFields[0]]) else number(row[self.coordFields[0]])
                lat = None if not number.isFloat(row[self.coordFields[1]]) else number(row[self.coordFields[1]])
                if len(self.coordFields) == 2:
                    alt = None if not number.isInt(row[self.coordFields[2]]) else number(row[self.coordFields[2]])
                else:
                    alt = None 
                self.coords.append(GXCoord(longitude = lon, latitude = lat, altitude = alt))
                if 'angleFields' in self.__dict__:
                    if len(self.angleFields) > 0:
                        heading = None if not number.isFloat(row[self.angleFields[0]]) else number(row[self.angleFields[0]])
                    else:
                        heading = None 
                    if len(self.angleFields) > 1:
                        tilt = None if not number.isFloat(row[self.angleFields[1]]) else number(row[self.angleFields[1]])
                    else:
                        tilt = None 
                    if len(self.angleFields) > 2:
                        roll = None if not number.isFloat(row[self.angleFields[2]]) else number(row[self.angleFields[2]])
                    else:
                        roll = None
                    if len(self.angleFields) > 0: 
                        self.angles.append(GXAngle(heading = heading, tilt = tilt, roll = roll))
        
    def __str__(self):
        tmp = self.indent + '<gx:Track{}>\n'.format(self.getID)
        if 'altitudeMode' in self.__dict__:
            tmp += str(self.altitudeMode)
        tmp += str(self.times)
        tmp += str(self.coords)
        tmp += str(self.angles)
        tmp += str(self.extendedData)
        tmp += self.indent + '</gx:Track>\n'
        return tmp

class Tracks(Container):
    def __init__(self):
        self.__permittedAttributes = []
        super().__init__(self.__permittedAttributes, [Track], False)
    
    def __str__(self):
        tmp = ''
        for i in range(len(self)):
            tmp += str(self[i])
        return tmp
    
class MultiTrack(KMLObject):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['interpolate', 'altitudeMode', 'tracks']
        super().__init__(self.__permittedAttributes)
        self.tracks = Tracks()
        self.tracks.depth = self.depth

    def __str__(self):
        tmp = self.indent + '<gx:MultiTrack{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</gx:MultiTrack\n'
        return tmp
        
class NetworkLink(KMLFeature):
    def __init__(self, **kwargs):
        
        self.__permittedAttributes = ['refreshVisibility','flyToView','link']
        super().__init__(self.__permittedAttributes, **kwargs)

    def __str__(self):
        tmp = self.indent + '<NetworkLink{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</NetworkLink>\n'
        return tmp
    
class Placemark(KMLFeature):
    def __init__(self, **kwargs):
        
        self.__permittedAttributes = ['geometry']
        super().__init__(self.__permittedAttributes, **kwargs)

    def __str__(self):
        tmp = self.indent + '<Placemark{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</Placemark>\n'
        return tmp
    
class KMLOverlay(KMLFeature):
    def __init__(self, **kwargs):
        self.__permittedAttributes = ['color', 'drawOrder', 'icon']
        super().__init__(self.__permittedAttributes)
    
class GroundOverlay(KMLOverlay):
    def __init__(self, **kwargs):
        
        self.__permittedAttributes = ['altitude', 'altitudeMode', 'latLonBox', 'latLonQuad']
        super().__init__(self.__permittedAttributes)

    def __str__(self):
        tmp = self.indent + '<GroundOverlay{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</GroundOverlay>\n'
        return tmp
    
class OverlayXY(Vector):
    def __init__(self, **kwargs):
        super().__init__([], **kwargs)
    
    def __str__(self):
        return self.indent + '<overlayXY {}/>\n'.format(super().__str__())
 
class ScreenXY(Vector):
    def __init__(self, **kwargs):
        super().__init__([], **kwargs)
    
    def __str__(self):
        return self.indent + '<screenXY {}/>\n'.format(super().__str__())
 
class RotationXY(Vector):
    def __init__(self, **kwargs):
        super().__init__([], **kwargs)
    
    def __str__(self):
        return self.indent + '<rotationXY {}/>\n'.format(super().__str__())
 
class Size(Vector):
    def __init__(self, **kwargs):
        super().__init__([], **kwargs)
    
    def __str__(self):
        return self.indent + '<size {}/>\n'.format(super().__str__())
 
class ScreenOverlay(KMLOverlay):
    def __init__(self, **kwargs):
        
        self.__permittedAttributes = ['overlayXY', 'screenXY', 'rotationXY', 'size', 'rotation']
        super().__init__(self.__permittedAttributes)

    def __str__(self):
        tmp = self.indent + '<ScreenOverlay{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</ScreenOverlay>\n'
        return tmp
    
class ViewVolume(KMLObject):
    def __init__(self, **kwargs):
        
        self.__permittedAttributes = ['leftFov', 'rightFov', 'bottomFov', 'topFov', 'near']
        super().__init__(self.__permittedAttributes)

    def __str__(self):
        tmp = self.indent + '<ViewVolume>\n'
        tmp += super().__str__()
        tmp += self.indent + '</ViewVolume>\n'
        return tmp
    
class ImagePyramid(KMLObject):
    def __init__(self, **kwargs):
        
        self.__permittedAttributes = ['tileSize', 'maxWidth', 'maxHeight', 'gridOrigin']
        super().__init__(self.__permittedAttributes)

    def __str__(self):
        tmp = self.indent + '<ImagePyramid>\n'
        tmp += super().__str__()
        tmp += self.indent + '</ImagePyramid>\n'
        return tmp
    
class PhotoOverlay(KMLObject):
    def __init__(self, **kwargs):
        
        self.__permittedAttributes = ['rotation', 'viewVolume', 'imagePyramid', 'point', 'shape']
        super().__init__(self.__permittedAttributes)

    def __str__(self):
        tmp = self.indent + '<PhotoOverlay{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</PhotoOverlay>\n'
        return tmp

class UpdateAttr(Container):
    def __init__(self):
        super().__init__([], [], False)
    
    def __str__(self):
        tmp = ''
        for i in range(len(self)):
            tmp += str(self[i])
        return tmp

class Update(KMLObject):
    def __init__(self, **kwargs):
        
        self.__permittedAttributes = ['targetHref', 'change', 'create', 'delete']
        super().__init__(self.__permittedAttributes)
        self.change = UpdateAttr()
        self.create = UpdateAttr()
        self.delete = UpdateAttr()

    def __str__(self):
        tmp = self.indent + '<Update{}>\n'.format(self.getID)
        if len(self.change) > 0:
            tmp += self.indent + ' <Change>\n'
            tmp += str(self.change)
            tmp += self.indent + ' </Change>\n'
        if len(self.create) > 0:
            tmp += self.indent + ' <Create>\n'
            tmp += str(self.create)
            tmp += self.indent + ' </Create>\n'
        if len(self.delete) > 0:
            tmp += self.indent + ' <Delete>\n'
            tmp += str(self.delete)
            tmp += self.indent + ' </Delete>\n'
            
        tmp += self.indent + '</Update>\n'
        return tmp

class GXTour(KMLFeature):
    def __init__(self, attributes, **kwargs):
        
        self.__permittedAttributes = attributes + ['playList']
        super().__init__(self.__permittedAttributes)

    def __str__(self):
        tmp = self.indent + '<gx:Tour{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</gx:Tour>\n'
        return tmp

class GXTourPrimitive(KMLObject):
    # Inheritance placeholder class for all tour types
    def __init__(self, permittedAttributes, **kwargs):
        self.__permittedAttributes = permittedAttributes
        super().__init__(self.__permittedAttributes, **kwargs)

class GXAnimatedUpdate(GXTourPrimitive):
    def __init__(self, **kwargs):
        
        self.__permittedAttributes = ['gx_duration', 'gx_delayedStart', 'update']
        super().__init__(self.__permittedAttributes)

    def __str__(self):
        tmp = self.indent + '<gx:AnimatedUpdate{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</gx:AnimatedUpdate>\n'
        return tmp

class GXFlyTo(GXTourPrimitive):
    def __init__(self, **kwargs):
        
        self.__permittedAttributes = ['gx_duration', 'gx_flyToMode', 'view']
        super().__init__(self.__permittedAttributes)

    def __str__(self):
        tmp = self.indent + '<gx:FlyTo{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</gx:FlyTo>\n'
        return tmp

class GXSoundCue(GXTourPrimitive):
    def __init__(self, **kwargs):
        
        self.__permittedAttributes = ['gx_delayedStart', 'href']
        super().__init__(self.__permittedAttributes)

    def __str__(self):
        tmp = self.indent + '<gx:SoundCue{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</gx:SoundCue>\n'
        return tmp

class GXTourControl(GXTourPrimitive):
    def __init__(self, **kwargs):
        
        self.__permittedAttributes = ['gx_playMode']
        super().__init__(self.__permittedAttributes)

    def __str__(self):
        tmp = self.indent + '<gx:TourControl{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</gx:TourControl>\n'
        return tmp

class GXWait(GXTourPrimitive):
    def __init__(self, **kwargs):
        
        self.__permittedAttributes = ['gx_duration']
        super().__init__(self.__permittedAttributes)

    def __str__(self):
        tmp = self.indent + '<gx:Wait{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</gx:Wait>\n'
        return tmp

class GXPlayList(Container):
    def __init__(self, **kwargs):
        
        self.__permittedAttributes = []
        super().__init__(self.__permittedAttributes, [GXTourPrimitive], False, **kwargs)

    def __str__(self):
        tmp = self.indent + '<gx:Playlist>\n'
        for i in range(len(self)):
            tmp += str(self[i])
        tmp += self.indent + '</gx:Playlist>\n'
        return tmp

class KML(KMLObject):
    def __init__(self, **kwargs):
        
        self.__permittedAttributes = ['hint', 'feature', 'save']
        super().__init__(self.__permittedAttributes)

    def __str__(self):
        tmp = self.indent + '<?xml version="1.0" encoding="utf-8"?>\n'
        tmp += self.indent + '<kml xmlns="http://www.opengis.net/kml/2.2" xmlns:gx="http://www.google.com/kml/ext/2.2"'
        if 'hint' in self.__dict__: tmp += '{}'.format(self.hint)
        tmp += '>\n'
        tmp += super().__str__()
        tmp += self.indent + '</kml>\n'
        return tmp
    
    def save(self, filename, overwrite = True):
        import os.path
        if os.path.isfile(filename):
            if not overwrite:
                raise FileError('File {} already exists and overwrite is False'.format(filename))
        with open(filename, 'w') as f:
            f.write(str(self))

class LinkSnippet(KMLObject):
    def __init__(self, **kwargs):
        super().__init__(['text','maxLines'], **kwargs)
        
    def __str__(self):
        if not self.checkAttributes(['text']):
            return ''
        else:
            tmp = self.indent + '<linkSnippet'
            if 'maxLines' in self.__dict__:
                tmp += ' maxLines="{}"'.format(self.maxLines)
            tmp += '>{}</linkSnippet>\n'.format(self.text)
            return tmp

class NetworkLinkControl(KMLObject):
    def __init__(self, attributes, **kwargs):
        
        self.__permittedAttributes = attributes + ['minRefreshPeriod', 'maxSessionLength', 'cookie', 
                                                   'message', 'linkName', 'linkDescription', 'linkSnippet',
                                                   'expires', 'update', 'view']
        super().__init__(self.__permittedAttributes)

    def __str__(self):
        tmp = self.indent + '<Template{}>\n'.format(self.getID)
        tmp += super().__str__()
        tmp += self.indent + '</Template>\n'
        return tmp

attributeTypes = {
    # Attribute name        : Data type
    'id'                    : str,
    'depth'                 : int,
    'latitude'              : angle90,
    'longitude'             : angle180,
    'altitude'              : number,
    'visibility'            : booleanEnum,
    'name'                  : str,
    'description'           : str,
    'open'                  : booleanEnum,
    'atom_link'             : ATOMLink,
    'link'                  : Link,
    'atom_author'           : ATOMAuthor,
    'address'               : str,
    'xal_AddressDetails'    : str,
    'phoneNumber'           : str,
    'Snippet'               : Snippet,
    'maxLines'              : int,
    'view'                  : KMLView,
    'begin'                 : KMLDateTime,
    'end'                   : KMLDateTime,
    'when'                  : KMLDateTime,
    'time'                  : TimePrimitive,
    'viewerOptions'         : GXViewerOptions,
    'gx_optionName'         : viewerOptionEnum,
    'enabled'               : booleanEnum,
    'heading'               : angle360,
    'tilt'                  : angle180,
    'roll'                  : angle180,
    'altitudeMode'          : altitudeMode,
    'altitudeModeEnum'      : altitudeModeEnum,
    'range'                 : number,
    'styleUrl'              : str,
    'color'                 : Color,
    'colorMode'             : colorModeEnum,
    'href'                  : str,
    'icon'                  : Icon,
    'x'                     : number,
    'y'                     : number,
    'xunits'                : hotspotUnitsEnum,
    'yunits'                : hotspotUnitsEnum,
    'hotSpot'               : HotSpot,
    'scale'                 : number,
    'key'                   : styleMapEnum,
    'width'                 : number,
    'gx_outerColor'         : Color,
    'gx_outerWidth'         : number,
    'gx_physicalWidth'      : number,
    'gx_labelVisibility'    : booleanEnum,
    'fill'                  : booleanEnum,
    'outline'               : booleanEnum,
    'bgColor'               : Color,
    'textColor'             : Color,
    'text'                  : str,
    'displayMode'           : displayModeEnum,
    'state'                 : itemIconModeEnum,
    'listItemType'          : listItemTypeEnum,
    'IconStyle'             : IconStyle,
    'LabelStyle'            : LabelStyle,
    'LineStyle'             : LineStyle,
    'PolyStyle'             : PolyStyle,
    'BalloonStyle'          : BalloonStyle,
    'ListStyle'             : ListStyle,
    'styleSelector'         : StyleSelector,
    'LatLonAltBox'          : LatLonAltBox,
    'Lod'                   : Lod,
    'north'                 : angle90,
    'south'                 : angle90,
    'east'                  : angle180,
    'west'                  : angle180,
    'minAltitude'           : number,
    'maxAltitude'           : number,
    'minLodPixels'          : number,
    'maxLodPixels'          : number,
    'minFadeExtent'         : number,
    'maxFadeExtent'         : number,
    'region'                : Region,
    'gx_x'                  : int,
    'gx_y'                  : int,
    'gx_h'                  : int,
    'gx_w'                  : int,
    'refreshMode'           : refreshModeEnum,
    'refreshInterval'       : number,
    'viewBoundScale'        : number,
    'viewFormat'            : str,
    'httpQuery'             : str,
    'type'                  : schemaTypeEnum,
    'displayName'           : str,
    'value'                 : str,
    'schema'                : Schema,
    'extendedData'          : ExtendedData,
    'schemaData'            : SchemaData,
    'viewRefreshMode'       : viewRefreshModeEnum,
    'viewRefreshTime'       : number,
    'z'                     : number,
    'sourceHref'            : str,
    'targetHref'            : str,
    'resourceMap'           : ResourceMap,
    'location'              : Location,
    'orientation'           : Orientation,
    'modelScale'            : Scale,
    'coordinates'           : Coordinates,
    'extrude'               : booleanEnum,
    'gx_altitudeOffset'     : number,
    'tessellate'            : booleanEnum,
    'gx_drawOrder'          : number,
    'linearRing'            : LinearRing,
    'outerBoundaryIs'       : OuterBoundary,
    'innerBoundaryIs'       : InnerBoundary,
    'csvFile'               : str,
    'csvparams'             : dict,
    'autoload'              : bool,
    'timeField'             : str,
    'coordFields'           : list,
    'timeFormat'            : str,
    'times'                 : TrackTimes,
    'coords'                : TrackCoords,
    'angles'                : TrackAngles,
    'angleFields'           : list,
    'model'                 : Model,
    'interpolate'           : booleanEnum,
    'tracks'                : Tracks,
    'refreshVisibility'     : booleanEnum,
    'flyToView'             : booleanEnum,
    'geometry'              : KMLGeometry,
    'drawOrder'             : number,
    'rotation'              : angle180,
    'latLonBox'             : LatLonBox,
    'latLonQuad'            : LatLonQuad,
    'overlayXY'             : OverlayXY,
    'screenXY'              : ScreenXY,
    'rotationXY'            : RotationXY,
    'size'                  : Size,
    'leftFov'               : angle180,
    'rightFov'              : angle180,
    'topFov'                : angle90,
    'bottomFov'             : angle90,
    'near'                  : number,
    'viewVolume'            : ViewVolume,
    'imagePyramid'          : ImagePyramid,
    'point'                 : Point,
    'shape'                 : shapeEnum,
    'gridOrigin'            : gridOriginEnum,
    'tileSize'              : number,
    'maxWidth'              : number,
    'maxHeight'             : number,
    'playList'              : GXPlayList,
    'change'                : UpdateAttr,
    'create'                : UpdateAttr,
    'delete'                : UpdateAttr,
    'gx_duration'           : number,
    'gx_delayedStart'       : number,
    'update'                : Update,
    'gx_flyToMode'          : flyToModeEnum,
    'gx_playMode'           : playModeEnum,
    'hint'                  : str,
    'feature'               : KMLFeature,
    'minRefreshPeriod'      : number,
    'maxSessionLength'      : number,
    'cookie'                : str,
    'message'               : str,
    'linkName'              : str,
    'linkDescription'       : str,
    'linkSnippet'           : LinkSnippet,
    'expires'               : KMLDateTime,
    'update'                : Update,
    'view'                  : KMLView,
}