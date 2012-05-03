#!/usr/bin/env python
#
#
import os, sys, base64, json, pprint, re, boto, syslog, pwd, logging, string, inspect, datetime


#
# Static class for logging. E.g. DXLog.warning("my warning")
# when DXLog.verbose() is called all warning messages are
# also written to stderr in additon to being captured in syslog
#
class DXLog:

    LEVELS = ('debug','info','notice','warn','error','critical','alert','emerg')
    logger = logging.getLogger(__name__)
    uname =  pwd.getpwuid(os.getuid())[0]
    program = os.path.basename(sys.argv[0])
    logger.setLevel(logging.DEBUG)
    stderr_formatter = logging.Formatter('%(program)s:%(message)s')
    dxlog_formatter = logging.Formatter('{ "login" : "%(uname)s", "source" : "%(program)s", "level" : "%(dxlevel)s", "timestamp", "%(isotime)s", "message" : "%(message)s" }')
    sh = logging.handlers.SysLogHandler("/dev/log")
    sh.setLevel(logging.DEBUG)
    sh.setFormatter(dxlog_formatter)
    logger.addHandler(sh)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(stderr_formatter)

    @classmethod
    def verbose(cls):
        cls.logger.addHandler(cls.ch)

    @classmethod
    def log(cls, level, message):
        if level == 'debug':
            DXLog.debug(message)
        elif level == 'info':
            DXLog.info(message)
        elif level == 'notice':
            DXLog.notice(message)
        elif level == 'warn':
            DXLog.warn(message)
        elif level == 'warning':
            DXLog.warn(message)
        elif level == 'error':
            DXLog.error(message)
        elif level == 'critical':
            DXLog.critical(message)
        elif level == 'alert':
            DXLog.alert(message)
        elif level == 'emerg':
            DXLog.emergency(message)
        elif level == 'emergency':
            DXLog.emergency(message)
        else:
            print "unrecognized level"
            

    @classmethod
    def debug(cls, message):
        d = { "uname" : cls.uname, "program" : cls.program, "isotime" : datetime.datetime.utcnow().isoformat(), "dxlevel" : "DEBUG" }   
        cls.logger.debug(message, extra=d )  

    @classmethod
    def info(cls, message):
        d = { "uname" : cls.uname, "program" : cls.program, "isotime" : datetime.datetime.utcnow().isoformat(), "dxlevel" : "INFO" }   
        cls.logger.info(message, extra=d )  
        
    @classmethod
    def notice(cls, message):
        d = { "uname" : cls.uname, "program" : cls.program, "isotime" : datetime.datetime.utcnow().isoformat(), "dxlevel" : "NOTICE" }   
        cls.logger.info(message, extra=d)

    @classmethod
    def warn(cls, message):
        DXLog.warning(message)

    @classmethod
    def warning(cls, message):
        d = { "uname" : cls.uname, "program" : cls.program, "isotime" : datetime.datetime.utcnow().isoformat(), "dxlevel" : "WARNING" }   
        cls.logger.warning(message, extra=d )  

    @classmethod
    def error(cls, message):
        d = { "uname" : cls.uname, "program" : cls.program, "isotime" : datetime.datetime.utcnow().isoformat(), "dxlevel" : "ERROR" }   
        cls.logger.error(message, extra=d )  

    @classmethod
    def critical(cls, message):
        d = { "uname" : cls.uname, "program" : cls.program, "isotime" : datetime.datetime.utcnow().isoformat(), "dxlevel" : "CRITICAL" }   
        cls.logger.critical(message, extra=d )  

    @classmethod
    def alert(cls, message):
        d = { "uname" : cls.uname, "program" : cls.program, "isotime" : datetime.datetime.utcnow().isoformat(), "dxlevel" : "ALERT" }   
        cls.logger.critical(message, extra=d )  

    @classmethod
    def emerg(cls, message):
        DXLog.emergency(message)

    @classmethod
    def emergency(cls, message):
        d = { "uname" : cls.uname, "program" : cls.program, "isotime" : datetime.datetime.utcnow().isoformat(), "dxlevel" : "EMERG" }   
        cls.logger.critical(message, extra=d)

    @classmethod
    def exception(cls, message):
        d = { "uname" : cls.uname, "program" : cls.program, "isotime" : datetime.datetime.utcnow().isoformat(), "dxlevel" : "ERROR" }   
        cls.logger.error(message, extra=d )  


