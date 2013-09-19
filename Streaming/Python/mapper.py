import sys

lines = sys.stdin.readlines()
for line in lines:
    words = line.split()
    for word in words:
	sys.stdout.write(word + "\n")
