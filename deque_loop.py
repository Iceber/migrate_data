from collections import deque
import  asyncio
import time

a  = (i for i in range(100))
f = open('tes','w')
q = deque(maxlen= 10)
def gen():
    while True:
        if len(q) < 5:
            for i in a:
                q.append(i)
                if len(q) == 10:
                    break
            else:
                break

def pr(gen_fu):
    while True:
        try:
            data = q.pop()
            print(data)
            time.sleep(0.1)
            f.write(str(data)+'\n')
        except IndexError:
            if gen_fu.done():
                break
            continue

loop = asyncio.get_event_loop()
gen_fu = loop.run_in_executor(None, gen)

pr_fu1 =loop.run_in_executor(None, pr, gen_fu)
pr_fu2 = loop.run_in_executor(None, pr, gen_fu)
loop.run_until_complete(asyncio.wait([pr_fu1,pr_fu2]))
loop.close()   
print('ok')
