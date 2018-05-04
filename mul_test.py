import multiprocessing,time
c=10
b = 3
def pr(i):
    print(c)

    time.sleep(1)
    return str(i)


def main():
    global c ,b
    c = 123
    b = 0
    pool = multiprocessing.Pool()
    async_result = pool.imap_unordered(pr,range(10))
    for result in async_result:
        print(result)
if __name__ == "__main__":
    print('start')
    main()
    print('bad')