#!/usr/bin/env python

import sys
import getopt
import cmd
import datetime
from boto import sqs, config

_DEFAULT_REGION = 'eu-west-1'
_QUEUE_NAMES = None

def _connect_to_region(region_name, **kwargs):
    for region in sqs.regions():
        if region.name == region_name:
            return region.connect(**kwargs)
    return None
# grumble stupid api - have some of *that*
sqs.connect_to_region = _connect_to_region

def _all_messages(queue, retry_count=30, page_size=10, visibility_timeout=None):
    retries = 0
    while retries <= retry_count and queue.count() > 0:
        messages = queue.get_messages(page_size, visibility_timeout)
        if not messages or len(messages) == 0:
            retries += 1
            continue
        else:
	    retries = 0
        for message in messages:
            yield message
                    
def _save_queue_to_file(queue, filename, delete=False, visibility_timeout=None):
    fp = open(filename, 'ab')
    n = 0
    for message in _all_messages(queue, retry_count=50, visibility_timeout=visibility_timeout):
        fp.write(message.get_body())
        fp.write('\n')
	if delete: queue.delete_message(message)
        n += 1
    fp.close()
    return n

def _get_queue_names(connection):
    global _QUEUE_NAMES
    if _QUEUE_NAMES is None:
        queues = connection.get_all_queues()
	_QUEUE_NAMES = [x.name for x in queues]
    return _QUEUE_NAMES

class SQSShell(cmd.Cmd):
    prompt = "[sqs] "
    _connection = None
    _queue = None
    
    def __init__(self, connection, queueName=None):
        self._connection = connection
        self.do_queue(queueName)
        cmd.Cmd.__init__(self)
        
    def _resetprompt(self):
        self.prompt = "[sqs:%s] " % (self._queue.name if self._queue else '')
    
    def do_queue(self, queueName):
        if queueName:
            queue = self._connection.lookup(queueName)
            if queue:
                self._queue = queue
                self._queue.set_message_class(sqs.message.RawMessage)
                self._resetprompt()
            else:
                print "Queue %s not found" % queueName
        elif queueName == '':
            self.do_list(None)
    do_q = do_queue

    def complete_queue(self, text, line, begidx, endidx):
        return [x for x in _get_queue_names(self._connection) if x.startswith(text)]
    complete_q = complete_queue

    def do_list(self, args):
        queues = self._connection.get_all_queues()
        _QUEUE_NAMES = [x.name for x in queues]
	print '\n'.join(_QUEUE_NAMES)
    do_l = do_list

    def do_attrs(self, attrnames):
        self.do_attr('All')
        
    def do_attr(self, attrname):
        if not self._queue:
            print "Please select a queue first"
            self.do_list(None)
        else:
            if not attrname:
                attrname = 'All'
            try:
                attrs = self._queue.get_attributes(attributes=attrname)
                for k, v in attrs.items():
                    print "%s: %s" % (k, v)
            except Exception, e:
                print 'Error: %s' % e
    do_a = do_attr

    def do_size(self, args):
        if not self._queue:
            print "Please select a queue first"
            self.do_list(None)
        else:
            print self._queue.count()
        
    do_count = do_size
    do_s = do_size
    
    def do_purge(self, args):
        if not self._queue:
            print "Please select a queue first"
            self.do_list(None)
        else:
            res = raw_input("Are you sure you want to PERMANENTLY DELETE all messages from the queue %s [No/yes]:" % self._queue.name)
            if res.lower() == 'yes':
                filename = raw_input("Please enter a local filename to store the messages: ")
                if filename:
                    count = _save_queue_to_file(self._queue, filename, delete=True)
                    print "%s queue messages saved and purged" % count
   
    def do_dump(self, args):
	if not self._queue:
            print "Please select a queue first"
	    self.do_list(None)
        else:
            res = raw_input("Are you sure you want to dump  all messages from the queue %s [No/yes]:" % self._queue.name)
            if res.lower() == 'yes':
                filename = raw_input("Please enter a local filename to store the messages: ")
                if filename:
                    count = _save_queue_to_file(self._queue, filename, visibility_timeout=30)
                    print "%s queue messages saved" % count

    def do_delete(self, queue):
        if not self._queue:
            print "Please select a queue first"
	    self.do_list(None)
	else:
            res = raw_input("Are you sure you want to DELETE the queue %s, including all of its messages?! [NO/yes]:" % self._queue.name)
	    if res.lower() == 'yes':
                res2 = raw_input("Really? This will delete all messages and the queue. Are you sure? [NO/yes]:")
		if res2.lower() == 'yes':
                    for msg in _all_messages(self._queue, visibility_timeout=5):
                        self._queue.delete_message(msg)
                    self._queue.delete()
		    self._queue = None
		    self._resetprompt()
		    print "Queue deleted"

 
    def do_peek(self, args):
        if not self._queue:
            print "Please select a queue first"
            self.do_list(None)
        else:
            if args:
                count = int(args)
            else:
                count = 1

            messages = self._queue.get_messages(num_messages=count, visibility_timeout=1, attributes='SentTimestamp')
            for message in messages:
		sentdate = datetime.datetime.fromtimestamp(int(message.attributes['SentTimestamp']) / 1000.0)
		print "ID: %s Sent: %s" % (message.id, sentdate)
                print message.get_body()
    
    def do_move(self, args):
        if not self._queue:
            print "Please select a queue first"
            self.do_list(None)
        else:
	    if not args:
		print "Please specify a target queue"
                return		
	    params = args.split()
	    if len(params) > 1:
	        (target, maxcount) = params[:2]
		maxcount = int(maxcount)
	    else:
		target = params[0]
		maxcount = None
            targetqueue = self._connection.lookup(target)
            if targetqueue:
		res = raw_input("Are you sure you want to MOVE %s messages from queue %s to queue %s [No/yes]:" % (maxcount if maxcount else 'all', self._queue.name, targetqueue.name))
                if res.lower() == 'yes':
                    targetqueue.set_message_class(sqs.message.RawMessage)
                    n = 0
                    for srcmessage in _all_messages(self._queue):
                        targetmsg = targetqueue.new_message(body=srcmessage.get_body())
                        targetmsg = targetqueue.write(targetmsg)
                        if targetmsg:
                            try:
                                if self._queue.delete_message(srcmessage):
                                    n += 1
                                else:
                                    raise Exception("Unable to delete message %s from source queue %s" % (srcmessage.id, self._queue.name))
                            except Exception:
                                try:
                                    if not targetqueue.delete_message(targetmsg):
                                        raise Exception
                                except Exception:
                                    print "FATAL: There is a duplicate message (%s) on queue %s that MUST BE DELETED. It is a duplicate of %s on queue %s" % (srcmessage.id, self._queue.name, targetmsg.id, targetqueue.name)
                                    break
			    if maxcount and n >= maxcount: break
                        else:
                            print "FATAL: Unable to write message %s:%s to queue %s. Queues have been left in a consistent state." % (self._queue.name, srcmessage.id, targetqueue.name)
                
                    print "%s messages moved from queue %s to queue %s" % (n, self._queue.name, targetqueue.name)
            else:
                print "Target queue %s not found" % target
        
    def do_EOF(self, args):
        print
        return True

def usage():
    print sys.argv[0], "[-a aws_access_key] [-s aws_secret_key] [-r aws_region] [-q sqs_queue_name]"
    
def main():
    """ options processing
    """

    options = "a:s:q:r:h"
    try:
        opts, args = getopt.getopt(sys.argv[1:], options)
    except getopt.GetoptError, err:
        usage()
        sys.exit(2)

    queueName = None
    regionName = _DEFAULT_REGION
    accessKey = config.get('Credentials', 'aws_access_key_id')
    secretKey = config.get('Credentials', 'aws_secret_access_key')

    for o, value in opts:
        if o == '-a':
            accessKey = value
        elif o == '-s':
            secretKey = value
        elif o == '-q':
            queueName = value
        elif o == '-r':
            regionName = value
        else:
            usage()
            if o == '-h':
                sys.exit(0)
            else:
                sys.exit(2)
    
    conn = sqs.connect_to_region(regionName, aws_access_key_id=accessKey, aws_secret_access_key=secretKey)
    if conn is not None:
        SQSShell(conn, queueName).cmdloop()
    else:
        print "Unable to connect to AWS %s with supplied credentials" % region_name
        sys.exit(2)

if __name__ == '__main__':
    main()
