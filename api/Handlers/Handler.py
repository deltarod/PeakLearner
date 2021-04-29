from api.util import PLdb as db
from simpleBDB import retry, txnAbortOnError

# https://realpython.com/python-interface/#using-metaclasses
class HandlerMeta(type):
    """A Handler metaclass used for creating handlers
    """
    handlers = {}

    def __init__(cls, name, bases, dct):
        cls.handlers[name] = cls
        cls.name = name

    def help(cls):
        """Handles the Help function"""
        if cls.name == 'Handler':
            outText = 'List of Available Handlers, and their commands:\n'
            for handler in cls.handlers:
                if handler == 'Handler':
                    continue
                outText += cls.handlers[handler].help() + '\n'
            # Remove last 2 new lines
            return outText[:-2]
        else:
            outText = '%s:\n' % cls.name
            commands = cls.getCommands()

            for command in commands:

                outText += '%s: %s\n' % (command, commands[command].__doc__)
            return outText


# TODO: Add Authentication
class Handler(metaclass=HandlerMeta):
    """
    Base Handler Class
    """
    def __init__(self, query):
        self.query = query

    # First decorator will retry the whole operation if the issue is a deadlock
    # The second decorator provides a txn that will be aborted upon exception, then the exception will be reraised
    @retry
    @txnAbortOnError
    def runCommand(self, method, data, *args, txn=None):
        command = self.methodCommands()[method]

        if callable(command):
            return command(self, data, *args, txn=txn)
        else:
            print(command, 'not yet implemented')
            return {}

    @classmethod
    def methodCommands(cls):
        return {'PUT': cls.do_PUT,
                'GET': cls.do_GET,
                'POST': cls.do_POST,
                'DELETE': cls.do_DELETE}

    # Override these
    def do_PUT(self, data, txn=None):
        print('do_PUT Not Yet Implemented for class', self.name)
        pass

    def do_GET(self, data, txn=None):
        print('do_GET Not Yet Implemented for class', self.name)
        pass

    def do_POST(self, data, txn=None):
        print('do_POST Not Yet Implemented for class', self.name)
        pass

    def do_DELETE(self, data, txn=None):
        print('do_DELETE Not Yet Implemented for class', self.name)
        pass


class TrackHandler(Handler):
    """
    Base Track Handler Class
    """
    key = 'TrackHandler'

    @classmethod
    def getHandlerByClassName(cls, name):
        return cls.handlers[name]

    @classmethod
    def getHandlerByKey(cls, key):
        for handler in cls.handlers:
            handlerToCheck = cls.handlers[handler]
            if issubclass(handlerToCheck, TrackHandler):
                if handlerToCheck.key == key:
                    return handlerToCheck
        return None

    def runCommandWithQuery(self, method, data):
        for key in self.query:
            data['args'][key] = self.query[key]

        return self.runCommand(method, data)
