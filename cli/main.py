#!/usr/bin/env python
# coding: utf-8
import sys
import argparse
import cmd
import readline
import os
import beanstalkc


argparser = argparse.ArgumentParser(description='Interactive beanstalk client', conflict_handler='resolve')
argparser.add_argument('-h', metavar='localhost', dest='host', type=str, default='localhost', help='hostname')
argparser.add_argument('-p', metavar='11300', dest='port', type=int, default=11300, help='port number')
argparser.add_argument('cmd_args', nargs=argparse.REMAINDER)
args = argparser.parse_args()

readline.parse_and_bind('tab: complete')
readline.parse_and_bind('set editing-mode vi')
histfile = os.path.join(os.path.expanduser("~"), ".beanstalk_cli_history")

try:
    readline.read_history_file(histfile)
except IOError:
    pass

import atexit
atexit.register(readline.write_history_file, histfile)


def silence(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print ('ERROR: %s' % str(e))
    return wrapper


class Cli(cmd.Cmd):
    def __init__(self):
        try:
            self.client = beanstalkc.Connection(host=args.host, port=args.port)
        except Exception as e:
            print str(e)
            sys.exit(-1)
        cmd.Cmd.__init__(self)
        self.prompt = 'beanstalk %s:%d (%s)> ' % (args.host, args.port, self.client.using())

    def do_hist(self, args):
        print(self._hist)

    def do_exit(self, args):
        self.client.close()
        return -1

    def do_EOF(self, args):
        return self.do_exit(args)

    def do_shell(self, args):
        os.system(args)

    def preloop(self):
        cmd.Cmd.preloop(self)
        self._hist = []

    def postloop(self):
        cmd.Cmd.postloop(self)

    def precmd(self, line):
        self._hist += [line.strip()]
        return line

    def postcmd(self, stop, line):
        return stop

    def emptyline(self):
        pass

    def default(self, line):
        print 'Bad command: %s' % line.split()[0]

    def do_quit(self, args):
        return -1

    @silence
    def do_stats(self, line):
        stats = self.client.stats()
        for k in stats:
            print '%s:%s' % (k, str(stats[k]))

    @silence
    def do_tubes(self, line):
        tubes = self.client.tubes()
        for t in tubes:
            print t

    @silence
    def do_use(self, line):
        self.client.use(line)
        print 'OK'
        self.prompt = 'beanstalk %s:%d (%s)> ' % (args.host, args.port, self.client.using())

    def complete_use(self, text, line, begidx, endidx):
        tubes = self.client.tubes()
        if text:
            return [t for t in tubes if t.startswith(text)]
        else:
            return tubes

    @silence
    def do_stats_tube(self, line):
        tube = self.client.using() if line.strip() == '' else line.strip()
        stats = self.client.stats_tube(tube)
        for k in stats:
            print '%s:%s' % (k, str(stats[k]))


def main():
    c = Cli()
    if len(args.cmd_args) > 0:
        line = ' '.join(args.cmd_args)
        c.preloop()
        c.precmd(line)
        c.onecmd(line)
        return
    c.cmdloop()


if __name__ == '__main__':
    main()
