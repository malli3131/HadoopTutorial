import sys

lines = sys.stdin.readlines()
wordcount = {};
for line in lines:
    if line in wordcount:
	count = wordcount[line]
	count = count + 1
	wordcount[line] = count
    else:
	wordcount[line] = 1
for word in wordcount:
    sys.stdout.write(word.strip()+ "\t" + str(wordcount[word]) + "\n")
