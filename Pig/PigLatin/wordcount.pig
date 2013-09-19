data = load '/pig/news';
words = foreach data generate flatten(TOKENIZE((chararray)$0)) as word;
tokens = filter words by word matches '\\w+';
gprd = group tokens by word;
cntd = foreach gprd generate group, COUNT(tokens) as fre;
ord = order cntd by fre desc;
lmt = limit ord 10;
dump lmt;
