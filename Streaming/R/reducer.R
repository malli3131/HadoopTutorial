#!/usr/bin/env Rscript
f <- file("stdin")
open(f)
while(length(line <- readLines(f,n=1)) > 0) {
	key <-  as.numeric(trim(strsplit(line, split="\t")[[1]][[2]]))	
  #write(key, stderr())
}
