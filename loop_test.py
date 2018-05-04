import asyncio

def pf (loop, i):
    while i != 0:
        i -= 1
        loop.run_in_executor(None,pr,i)

def pr(i):
    print(i)

def test():
    print('start')
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None,pf, loop, 10)
    loop.run_forever()
    print('end')
if __name__ == "__main__":
    test()