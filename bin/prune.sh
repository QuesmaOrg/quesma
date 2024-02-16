#!/bin/bash
# Prune docker which you need to do a lot on Mac
set -e

docker system prune -a

echo "Please also purge data from Docker for Mac by going to Troubleshoot > Purge Data."
echo "Instructions: https://www.notion.so/Known-issues-b829a92df063434ca65cfe51dd708ad6"
read -p "Press any key to continue. " -n 1 -r
