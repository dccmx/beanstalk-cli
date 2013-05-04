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


def print_yaml(yaml):
    for k in sorted(yaml.keys()):
        print '%s:%s' % (k, str(yaml[k]))


class Cli(cmd.Cmd):
    def __init__(self):
        try:
            self.client = beanstalkc.Connection(host=args.host, port=args.port)
        except Exception as e:
            print str(e)
            sys.exit(-1)
        cmd.Cmd.__init__(self)
        self.job = None
        self._refresh_prompt()

    def _refresh_prompt(self):
        prompt = 'beanstalk %s:%d' % (args.host, args.port)
        if self.job is not None:
            prompt += ' (%s:%d)' % (self.client.using(), self.job.jid)
        else:
            prompt += ' (%s)' % self.client.using()
        self.prompt = prompt + '> '

    @property
    def tubes(self):
        return sorted(self.client.tubes())

    @property
    def watching(self):
        return sorted(self.client.watching())

    def _complete_tube(self, text, line, begidx, endidx):
        tubes = self.tubes
        if text:
            return [t for t in tubes if t.startswith(text)]
        else:
            return tubes

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
        print_yaml(self.client.stats())

    @silence
    def do_tubes(self, line):
        for t in self.tubes:
            tube_stat = self.client.stats_tube(t)
            ts = '(buried: %d, delayed: %d, ready: %d, reserved: %d, urgent: %d)' % (tube_stat['current-jobs-buried'],
                                                                                     tube_stat['current-jobs-delayed'],
                                                                                     tube_stat['current-jobs-ready'],
                                                                                     tube_stat['current-jobs-reserved'],
                                                                                     tube_stat['current-jobs-urgent'])
            print t, ts

    @silence
    def do_use(self, line):
        self.client.use(line)
        self._refresh_prompt()
        print 'OK'

    def complete_use(self, text, line, begidx, endidx):
        return self._complete_tube(text, line, begidx, endidx)

    @silence
    def do_stats_tube(self, line):
        tube = self.client.using() if line == '' else line
        print_yaml(self.client.stats_tube(tube))

    @silence
    def do_watch(self, line):
        self.client.watch(line)
        print 'OK, Current watching:', ','.join(self.watching)

    def complete_watch(self, text, line, begidx, endidx):
        return self._complete_tube(text, line, begidx, endidx)

    @silence
    def do_ignore(self, line):
        self.client.ignore(line)
        print 'OK, Current watching:', ','.join(self.watching)

    def complete_ignore(self, text, line, begidx, endidx):
        tubes = self.watching
        if text:
            return [t for t in tubes if t.startswith(text)]
        else:
            return tubes

    @silence
    def do_watching(self, line):
        print ','.join(self.watching)

    @silence
    def do_put(self, line):
        print self.client.put(line)

    @silence
    def do_reserve(self, line):
        timeout = None if line == '' else float(line)
        job = self.client.reserve(timeout)
        if job is None:
            print 'No job now'
            return
        self.job = job
        self._refresh_prompt()
        print_yaml(job.stats())

    @silence
    def do_stats_job(self, line):
        if line != '':
            print_yaml(self.client.stats_job(int(line)))
        elif self.job is not None:
            print_yaml(self.job.stats())
        else:
            print 'No job reserved now'

    '''
    peek <id>\r\n - return job <id>.
    peek-ready\r\n - return the next ready job.
    peek-delayed\r\n - return the delayed job with the shortest delay left.
    peek-buried\r\n - return the next job in the list of buried jobs.
    All but the first operate only on the currently used tube.
    '''
    @silence
    def do_peek(self, line):
        if line.strip() == '':
            print 'Usage: peek id'
            return
        job = self.client.peek(int(line))
        if job is None:
            print 'No such job'
            return
        print_yaml(job.stats())

    @silence
    def do_peek_ready(self, line):
        job = self.client.peek_ready()
        if job is None:
            print 'No job ready now'
            return
        print_yaml(job.stats())

    @silence
    def do_peek_delayed(self, line):
        job = self.client.peek_delayed()
        if job is None:
            print 'No job delayed now'
            return
        print_yaml(job.stats())

    @silence
    def do_peek_buried(self, line):
        job = self.client.peek_buried()
        if job is None:
            print 'No job buried now'
            return
        print_yaml(job.stats())

    def _clear_all(self, tube, peek_job):
        using = self.client.using()
        self.client.use(tube)
        total = 0
        while True:
            job = peek_job()
            if job is None:
                break
            job.delete()
            total += 1
        self.client.use(using)
        return total

    def _do_clear(self, line, name, peek_job):
        args = line.split()
        if len(args) == 0:
            yes = raw_input('Clear all %s jobs in %s now? (y/N)' % (name, self.client.using()))
            if not yes == 'y':
                return
            total = self._clear_all(self.client.using(), peek_job)
        else:
            force = False
            if len(args) >= 1:
                tube = args[0]
            if len(args) >= 2:
                force = args[1] == '-f'
            if not force:
                yes = raw_input('Clear all %s jobs in %s now? (y/N)' % (name, tube))
                if not yes == 'y':
                    return
            total = self._clear_all(tube, peek_job)
        if total > 0:
            print 'OK, %d %s jobs cleared!' % (total, name)
        else:
            print 'No %s jobs to be cleared now' % name

    @silence
    def do_clear_buried(self, line):
        self._do_clear(line, 'buried', self.client.peek_buried)

    def complete_clear_buried(self, text, line, begidx, endidx):
        return self._complete_tube(text, line, begidx, endidx)

    @silence
    def do_clear_delayed(self, line):
        self._do_clear(line, 'delayed', self.client.peek_delayed)

    def complete_clear_delayed(self, text, line, begidx, endidx):
        return self._complete_tube(text, line, begidx, endidx)

    @silence
    def do_clear_ready(self, line):
        self._do_clear(line, 'ready', self.client.peek_ready)

    def complete_clear_ready(self, text, line, begidx, endidx):
        return self._complete_tube(text, line, begidx, endidx)


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
