import time
import os

def create_file(name,size):
    with open(name,'wb') as f:
        f.seek(size-1)
        f.write('\0')
        f.close()

if __name__ == '__main__':
    path = os.path.expanduser("~/Downloads/")
    created_files = []

    print "pdf with size 1k"
    fp = os.path.join(path,"test.pdf")
    created_files.append(fp)
    create_file(fp,1024)
    time.sleep(0.5)

    print "pdf with size 1mb"
    fp = os.path.join(path,"test1.pdf")
    created_files.append(fp)
    create_file(fp,1024**2)
    time.sleep(0.5)

    time.sleep(1.0)
    for fp in created_files:
        if os.path.exists(fp):
            os.remove(fp)
    print "test files removed"


    