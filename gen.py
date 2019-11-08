import datetime
import random
import sys
seed = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()_+=-"
random.seed(datetime.datetime.now())
n = int(sys.argv[1])
m = int(sys.argv[2])
for j in range(n):
  content = []
  for i in range(m):
    content.append(random.choice(seed))
  result = ''.join(content)
  file = open("{}.obj".format(j + 1), "w")
  file.write(result)
  file.close()