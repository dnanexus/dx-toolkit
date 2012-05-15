import socket, json, time, os, sys


#
# Static class for AppLog.logging. E.g. DXLog.warning("my warning")
# when DXLog.verbose() is called all warning messages are
# also written to stderr in additon to being captured in sysAppLog.log
#
class AppLog:
    @staticmethod
    def log(message, level = 6): 
      socketFile = "/opt/dnanexus/log/bulk"
      if (level < 3):  
        socketFile = "/opt/dnanexus/log/priority"

      if (not os.path.exists(socketFile)): 
        print >> sys.stderr, "Socket " + socketFile + " does not exist"
        return False

      s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
      s.connect(socketFile)

      data = {"source": "app", "timestamp": int(round(time.time() *1000)), "level": level, "msg": message}
      s.send(json.dumps(data))
      s.close()
      time.sleep(0.0011)
      return True
            
    @staticmethod
    def emerg(message):
      return AppLog.log(message, 0)

    @staticmethod
    def alert(message):
      return AppLog.log(message, 1)

    @staticmethod
    def crit(message):
      return AppLog.log(message, 2)

    @staticmethod
    def error(message):
      return AppLog.log(message, 3)

    @staticmethod
    def warn(message):
      return AppLog.log(message, 4)

    @staticmethod
    def notice(message):
      return AppLog.log(message, 5)

    @staticmethod
    def info(message):
      return AppLog.log(message, 6)

    @staticmethod
    def debug(message):
      return AppLog.log(message, 7)
