import logging

class ColoredFormatter(logging.Formatter):
    def __init__(self, msg):
        self.length = 20
        self.colors = {'WARNING':34,'CRITICAL':31,'ERROR':31}
        self.reset_seq = '\033[0m'
        self.color_seq = '\033[1;%dm'
        logging.Formatter.__init__(self, msg)
    def format(self, record):
        if record.levelname in self.colors:
            l = record.levelname
            record.levelname = '%-20s'%( (self.color_seq % self.colors[l]) + l + self.reset_seq)
            record.msg =  self.color_seq % (self.colors['CRITICAL']) + record.msg + self.reset_seq
        else:
            l = self.length - len(self.reset_seq) - len(self.color_seq)
            record.levelname = ('%%-%ds'%l) % record.levelname
        return logging.Formatter.format(self, record)

class ColoredLogger(logging.Logger):
    message = '%(levelname)s[ %(name)-15s ] %(message)s'
    def __init__(self, name):
        logging.Logger.__init__(self, name, logging.INFO)
        console = logging.StreamHandler()
        console.setFormatter(ColoredFormatter(self.message))
        self.addHandler(console)
        return

# these seems to be necessary to avoid duplicate logs:
if len(logging._handlers)==0:
  logging.setLoggerClass(ColoredLogger)

