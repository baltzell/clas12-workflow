#!/bin/bash

# files greater than 10 MB, modified within the past week:
find $1 -type f -size +10M -mtime -7 

# files less than 10 MB, modified with the past 12 hours:
#find $1 -type f -size -10M -mtime -0.5

#find $1 -type f -size -10M -exec ls -lh {} +

