import sys
import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class nyzr_handler(FileSystemEventHandler):
    def on_moved(self, event):
        super(nyzr_handler, self).on_moved(event)

        what = 'directory' if event.is_directory else 'file'
        print "Moved %s: from %s to %s" % (what, event.src_path, event.dest_path)

    def on_created(self, event):
        super(nyzr_handler, self).on_created(event)

        what = 'directory' if event.is_directory else 'file'
        print "Created %s: %s" % (what, event.src_path)

    def on_deleted(self, event):
        super(nyzr_handler, self).on_deleted(event)

        what = 'directory' if event.is_directory else 'file'
        print "Deleted %s: %s" % (what, event.src_path)

    def on_modified(self, event):
        super(nyzr_handler, self).on_modified(event)

        what = 'directory' if event.is_directory else 'file'
        print "Modified %s: %s" % (what, event.src_path)
        


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    path = sys.argv[1] if len(sys.argv) > 1 else '.'
    nyzr = nyzr_handler()
    observer = Observer()
    observer.schedule(nyzr, path, recursive=True)
    observer.start()
    print observer.ident
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
